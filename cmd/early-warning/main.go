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

	// --- RabbitMQ (separate connections for consume and publish) ---
	amqpURL := amqpURL()
	consumeConn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer func() { _ = consumeConn.Close() }()
	consumeCh, err := consumeConn.Channel()
	if err != nil {
		return err
	}
	publishConn, err := amqp.Dial(amqpURL)
	if err != nil {
		return err
	}
	defer func() { _ = publishConn.Close() }()
	publishCh, err := publishConn.Channel()
	if err != nil {
		return err
	}

	// --- Firebase / push ---
	pusher, err := publisher.NewFirebasePusher(ctx, publisher.FirebaseConfig{
		CredentialsPath: config.String("FIREBASE_CREDENTIALS_PATH", ""),
		CredentialsJSON: config.String("FIREBASE_CREDENTIALS_JSON", ""),
	})
	if err != nil {
		log.Warn("firebase_init_failed", "err", err.Error())
	}

	pub := publisher.New(publisher.Config{
		AlertsExchange: config.String("ALERTS_EXCHANGE", "sensors.raw"),
		HTTPAlertURL:   config.String("ALERT_HTTP_URL", ""),
		HTTPTimeout:    config.Duration("HTTP_TIMEOUT", 5*time.Second),
	}, publishCh, pusher, log)

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

	// --- Consumer ---
	cons := consumer.New(consumer.Deps{
		Channel:    consumeCh,
		Queue:      config.String("RABBITMQ_QUEUE", "early_warning.evaluate"),
		Engine:     engine,
		Cache:      redisCache,
		Store:      st,
		Publisher:  pub,
		Subs:       resolver,
		Levels:     levels,
		Idemp:      idemp,
		MaxRetries: config.Int("CONSUMER_MAX_RETRIES", 3),
		Metrics:    mx,
		Log:        log,
	})
	go func() {
		if err := cons.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("consumer_stopped", "err", err.Error())
		}
	}()

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
