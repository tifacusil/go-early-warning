// Command early-warning is the entrypoint for the early-warning microservice.
// It wires config, Redis, MySQL, RabbitMQ, the threshold engine, publisher,
// subscription resolver, HTTP API, the report scheduler and Consul registration.
// All settings come from the environment (CLAUDE.md §1.7).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/AngelloFD/go-early-warning/internal/earlywarning/api"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/cache"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/consumer"
	dav1 "github.com/AngelloFD/go-early-warning/internal/earlywarning/grpc/dataanalysis/v1"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/publisher"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/store"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/subs"
	"github.com/AngelloFD/go-early-warning/internal/earlywarning/threshold"
	"github.com/AngelloFD/go-early-warning/pkg/config"
	"github.com/AngelloFD/go-early-warning/pkg/consul"
	"github.com/AngelloFD/go-early-warning/pkg/grpcx"
	"github.com/AngelloFD/go-early-warning/pkg/idempotency"
	"github.com/AngelloFD/go-early-warning/pkg/logging"
	"github.com/AngelloFD/go-early-warning/pkg/metrics"
)

const serviceName = "early-warning"

// version is set at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	log := logging.New(serviceName, config.String("INSTANCE_ID", "ew-1"), config.String("LOG_LEVEL", "info"))
	log.Info("starting", "version", version)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, log); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, log *slog.Logger) error {
	// --- Metrics (Prometheus /metrics, CLAUDE.md §5) ---
	mx := metrics.New(serviceName)

	// --- Redis (compartido en infra: host "redis", auth REDIS_PASSWORD) ---
	redisCache := cache.New(cache.Config{
		Host: config.String("REDIS_HOST", "redis"),
		Port: config.Int("REDIS_PORT", 6379),
		Pass: config.String("REDIS_PASSWORD", ""),
	})

	// --- MySQL (BD exclusiva del servicio, database-per-service §1.2) ---
	st, err := store.New(store.Config{
		Host: config.String("MYSQL_HOST", "mysql-early-warning"),
		Port: config.Int("MYSQL_PORT", 3306),
		User: config.String("MYSQL_USER", "svc_early_warning"),
		Pass: config.String("MYSQL_PASSWORD", ""),
		DB:   config.String("MYSQL_DATABASE", "early_warning"),
	})
	if err != nil {
		return err
	}
	defer func() { _ = st.Close() }()

	// --- Migraciones (idempotentes; golang-migrate salta las aplicadas) ---
	if config.Bool("RUN_MIGRATIONS", true) {
		if err := st.Migrate(); err != nil {
			return fmt.Errorf("migrations: %w", err)
		}
		log.Info("migrations_applied")
	}

	// --- RabbitMQ (conexion supervisada) ---
	// El dial real ocurre dentro de superviseConsumer, que reconecta con backoff
	// si el broker cae. Antes se dialeaba una sola vez aca: una caida de RabbitMQ
	// cerraba el canal de deliveries, el consumer retornaba y el proceso quedaba
	// vivo (pod Ready) pero sin consumir hasta un reinicio manual del pod.
	amqpURL := amqpURL()

	// --- Firebase / push ---
	pusher, err := publisher.NewFirebasePusher(ctx, publisher.FirebaseConfig{
		CredentialsPath: config.String("FIREBASE_CREDENTIALS_PATH", ""),
		CredentialsJSON: config.String("FIREBASE_CREDENTIALS_JSON", ""),
	})
	if err != nil {
		log.Warn("firebase_init_failed", "err", err.Error())
	} else if publisher.IsNoop(pusher) {
		// Silent otherwise: alerts keep being generated and "alert_emitted" logged,
		// but no FCM push is ever attempted. Loud on purpose so this isn't mistaken
		// for a working push pipeline.
		log.Warn("firebase_not_configured_push_disabled")
	}

	pubCfg := publisher.Config{
		AlertsExchange: config.String("ALERTS_EXCHANGE", "sensors.raw"),
		HTTPAlertURL:   config.String("ALERT_HTTP_URL", ""),
		HTTPTimeout:    config.Duration("HTTP_TIMEOUT", 5*time.Second),
	}
	// El canal de publish se reconstruye en cada reconexion (misma conexion que
	// el consume), asi el fan-out de alertas a RabbitMQ tambien sobrevive la caida.
	buildPublisher := func(ch *amqp.Channel) *publisher.Publisher {
		return publisher.New(pubCfg, ch, pusher, log)
	}

	// --- gRPC: user-management (subscriptions) ---
	umConn, err := grpc.NewClient(config.String("USER_MANAGEMENT_GRPC", "user-management-1:9090"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = umConn.Close() }()
	resolver := subs.New(umConn, grpcx.New(grpcx.Options{
		Timeout:       time.Duration(config.Int("USER_MANAGEMENT_TIMEOUT_MS", 5000)) * time.Millisecond,
		MaxRetries:    2,
		FailThreshold: config.Int("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5),
		OpenFor:       time.Duration(config.Int("CIRCUIT_BREAKER_RESET_TIMEOUT_MS", 30000)) * time.Millisecond,
	}))

	// --- Threshold engine + project levels ---
	engine := threshold.DefaultConfig(float64(config.Int("MAX_DT_SECONDS", 7200)))
	levels := defaultLevels()

	// --- Idempotency store (dedup by message_id, CLAUDE.md §1.3) ---
	idemp := idempotency.NewRedisStore(redisCache.Client(), "early_warning:idemp:",
		config.Duration("IDEMPOTENCY_TTL", 24*time.Hour))

	// --- Consumer (supervisado: reconecta al broker con backoff) ---
	queue := config.String("RABBITMQ_QUEUE", "early_warning.evaluate")
	maxRetries := config.Int("CONSUMER_MAX_RETRIES", 3)
	// Concurrencia del consumidor. Sharded por sensor_id, así que subirlo no
	// rompe el orden por sensor; 1 = orden global estricto (comportamiento viejo).
	workers := config.Int("CONSUMER_WORKERS", 8)
	prefetch := config.Int("CONSUMER_PREFETCH", 0) // 0 => workers*8, mín. 20
	buildConsumer := func(consumeCh *amqp.Channel, pub *publisher.Publisher) *consumer.Consumer {
		return consumer.New(consumer.Deps{
			Channel:    consumeCh,
			Queue:      queue,
			Engine:     engine,
			Cache:      redisCache,
			Store:      st,
			Publisher:  pub,
			Subs:       resolver,
			Levels:     levels,
			Idemp:      idemp,
			MaxRetries: maxRetries,
			Metrics:    mx,
			Log:        log,
			Workers:    workers,
			Prefetch:   prefetch,
		})
	}
	go superviseConsumer(ctx, log, amqpURL, buildPublisher, buildConsumer)

	// --- gRPC: data-analysis (enriquecimiento de reportes) ---
	daConn, err := grpc.NewClient(config.String("DATA_ANALYSIS_GRPC", "data-analysis-1:50051"),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = daConn.Close() }()
	daClient := dav1.NewAnalysisServiceClient(daConn)
	daCaller := grpcx.New(grpcx.Options{
		Timeout:       time.Duration(config.Int("DATA_ANALYSIS_TIMEOUT_MS", 5000)) * time.Millisecond,
		MaxRetries:    2,
		FailThreshold: config.Int("CIRCUIT_BREAKER_FAILURE_THRESHOLD", 5),
		OpenFor:       time.Duration(config.Int("CIRCUIT_BREAKER_RESET_TIMEOUT_MS", 30000)) * time.Millisecond,
	})

	// --- HTTP API ---
	srv := api.New(api.Deps{
		Service:        serviceName,
		Cache:          redisCache,
		Store:          st,
		DataAnalysis:   daClient,
		DataAnalysisDo: daCaller,
		Metrics:        mx,
		Log:            log,
		HealthProbes: map[string]func(context.Context) error{
			"redis": redisCache.Ping,
			"mysql": st.Ping,
		},
	})
	httpSrv := &http.Server{
		Addr:    ":" + config.String("HTTP_PORT", "8080"),
		Handler: srv.Routes(),
	}

	// --- Report scheduler ---
	if config.Bool("REPORT_SCHEDULER_ENABLED", true) {
		sched := api.NewScheduler(srv, projectIDs(levels),
			config.Duration("REPORT_CHECK_INTERVAL", time.Minute), log)
		go sched.Run(ctx)
	}

	// --- Consul registration (best effort) ---
	if config.Bool("CONSUL_ENABLED", true) {
		registerConsul(log)
	}

	go func() {
		<-ctx.Done()
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpSrv.Shutdown(shutCtx)
	}()

	log.Info("http_listening", "addr", httpSrv.Addr)
	if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return ctx.Err()
}

func amqpURL() string {
	return "amqp://" +
		config.String("RABBITMQ_USER", "guest") + ":" +
		config.String("RABBITMQ_PASSWORD", "guest") + "@" +
		config.String("RABBITMQ_HOST", "127.0.0.1") + ":" +
		strconv.Itoa(config.Int("RABBITMQ_PORT", 5672)) + "/"
}

// superviseConsumer mantiene el consumo vivo reconectando al broker con backoff
// exponencial (cap 30s) cada vez que la conexion cae. Solo retorna cuando ctx se
// cancela. Cierra el gap de resiliencia medido con chaos/kill-rabbitmq: matar
// RabbitMQ ya no deja el consumer muerto hasta un reinicio manual del pod
// (tolerancia a fallos, CLAUDE.md 2.1/2.2). El dial inicial tambien reintenta,
// asi que el servicio tolera arrancar antes que el broker.
func superviseConsumer(ctx context.Context, log *slog.Logger, url string,
	buildPublisher func(*amqp.Channel) *publisher.Publisher,
	buildConsumer func(*amqp.Channel, *publisher.Publisher) *consumer.Consumer) {
	const (
		baseBackoff = 1 * time.Second
		maxBackoff  = 30 * time.Second
	)
	backoff := baseBackoff
	for {
		if ctx.Err() != nil {
			return
		}
		connected, err := consumeSession(ctx, url, buildPublisher, buildConsumer)
		if ctx.Err() != nil {
			return
		}
		if connected {
			// La sesion vivio: la proxima reconexion arranca rapido.
			backoff = baseBackoff
			log.Warn("consumer_reconnecting", "err", errString(err), "backoff", backoff.String())
		} else {
			log.Warn("rabbitmq_connect_failed_retrying", "err", errString(err), "backoff", backoff.String())
		}
		if !sleepCtx(ctx, backoff) {
			return
		}
		backoff = nextBackoff(backoff, maxBackoff)
	}
}

// consumeSession abre una conexion, construye publisher + consumer y consume
// hasta que la conexion cae o ctx se cancela. connected indica si el dial y los
// canales se establecieron (para resetear el backoff tras una sesion real).
func consumeSession(ctx context.Context, url string,
	buildPublisher func(*amqp.Channel) *publisher.Publisher,
	buildConsumer func(*amqp.Channel, *publisher.Publisher) *consumer.Consumer) (connected bool, err error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return false, fmt.Errorf("dial: %w", err)
	}
	defer func() { _ = conn.Close() }()
	consumeCh, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("consume channel: %w", err)
	}
	publishCh, err := conn.Channel()
	if err != nil {
		return false, fmt.Errorf("publish channel: %w", err)
	}
	pub := buildPublisher(publishCh)
	cons := buildConsumer(consumeCh, pub)

	closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))
	runErr := make(chan error, 1)
	go func() { runErr <- cons.Run(ctx) }()

	select {
	case <-ctx.Done():
		return true, ctx.Err()
	case e := <-runErr:
		if e == nil {
			e = consumer.ErrConnClosed
		}
		return true, e
	case amqpErr := <-closeCh:
		if amqpErr == nil {
			return true, consumer.ErrConnClosed
		}
		return true, fmt.Errorf("%w: %v", consumer.ErrConnClosed, amqpErr)
	}
}

func nextBackoff(cur, max time.Duration) time.Duration {
	if n := cur * 2; n < max {
		return n
	}
	return max
}

// sleepCtx duerme d, o retorna false si ctx se cancela antes.
func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func errString(err error) string {
	if err == nil {
		return "(nil)"
	}
	return err.Error()
}

// staticLevels is a placeholder ThresholdProvider until project-management (Hito 5).
type staticLevels map[string]*threshold.Levels

func (s staticLevels) Levels(projectID string) *threshold.Levels { return s[projectID] }

// projectIDs lists the known projects (scheduler iterates these).
func projectIDs(s staticLevels) []string {
	ids := make([]string, 0, len(s))
	for id := range s {
		ids = append(ids, id)
	}
	return ids
}

// defaultLevels ports config.THRESHOLDS.
// TODO(SE-practice): replace with a project-management client (Hito 5); thresholds
// must not live in service config (CLAUDE.md §1.7).
func defaultLevels() staticLevels {
	return staticLevels{
		"shangtan": {Red: 20, Orange: 15, Blue: 10},
		"lingzhan": {Red: 15, Orange: 10, Blue: 5},
	}
}

func registerConsul(log *slog.Logger) {
	cl, err := consul.NewClient(consul.AgentConfig{
		Host:   config.String("CONSUL_HOST", "consul-1"),
		Port:   config.Int("CONSUL_PORT", 8500),
		Scheme: config.String("CONSUL_SCHEME", "http"),
		Token:  config.String("CONSUL_TOKEN", ""),
	})
	if err != nil {
		log.Warn("consul_setup_failed", "err", err.Error())
		return
	}
	// En infra cada instancia se anuncia por su hostname (early-warning-1/-2),
	// alcanzable en la red landslide-net en HTTP_PORT.
	instance := config.String("INSTANCE_ID", "early-warning-1")
	address := config.String("CONSUL_SERVICE_ADDRESS", instance)
	port := config.Int("HTTP_PORT", 8080)
	if err := cl.Register(consul.Registration{
		ServiceName:     serviceName,
		ServiceID:       serviceName + "-" + instance,
		Address:         address,
		Port:            port,
		Tags:            []string{"lang-go", "version-1", "env-" + config.String("ENV", "dev")},
		HealthURL:       "http://" + address + ":" + strconv.Itoa(port) + "/health",
		HealthInterval:  10 * time.Second,
		HealthTimeout:   5 * time.Second,
		DeregisterAfter: time.Minute,
	}); err != nil {
		log.Warn("consul_register_failed", "err", err.Error())
	}
}
