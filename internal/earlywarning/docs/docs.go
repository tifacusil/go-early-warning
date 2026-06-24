// Package docs sirve la especificación OpenAPI y una UI de Swagger sobre el
// http.ServeMux del servicio (sin dependencias nuevas: los assets de Swagger UI
// se cargan desde un CDN).
//
// El spec YAML se embebe con go:embed; go:embed no permite rutas con "..", así
// que se embebe la copia local internal/earlywarning/docs/openapi.yaml. La
// fuente canónica es contracts/openapi/early-warning.yaml (mantener en sync).
package docs

import (
	_ "embed"
	"net/http"
	"strings"
)

//go:embed openapi.yaml
var openapiSpec []byte

// swaggerUI apunta al spec con URL RELATIVA ("openapi.yaml"), que el navegador
// resuelve contra el directorio actual. Robusta tras cualquier reverse proxy /
// prefix (p.ej. /api/v1/warnings/docs/) sin recompilar; por eso la UI debe
// servirse con barra final (/docs/).
const swaggerUI = `<!DOCTYPE html>
<html lang="es">
<head>
  <meta charset="UTF-8">
  <title>early-warning — API docs</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js" crossorigin></script>
  <script>
    window.onload = () => {
      window.ui = SwaggerUIBundle({
        url: "openapi.yaml",
        dom_id: "#swagger-ui",
      });
    };
  </script>
</body>
</html>`

// Register monta la UI y el spec en el mux dado:
//
//	GET /docs            -> redirect a docs/ (barra final)
//	GET /docs/           -> Swagger UI
//	GET /docs/openapi.yaml -> spec
func Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /docs", redirectToSlash)
	mux.HandleFunc("GET /docs/", serveUI)
	mux.HandleFunc("GET /docs/openapi.yaml", serveSpec)
}

// redirectToSlash manda /docs -> docs/ con Location RELATIVO, para preservar el
// prefix del gateway (p.ej. /api/v1/warnings/docs/) sin absolutizar.
func redirectToSlash(w http.ResponseWriter, r *http.Request) {
	p := r.URL.Path
	w.Header().Set("Location", lastSegment(p)+"/")
	w.WriteHeader(http.StatusMovedPermanently)
}

func serveUI(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(swaggerUI))
}

func serveSpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/yaml")
	_, _ = w.Write(openapiSpec)
}

// lastSegment devuelve el último segmento de un path (sin barras), p.ej.
// "/docs" -> "docs". Se usa para construir un redirect relativo.
func lastSegment(p string) string {
	p = strings.TrimRight(p, "/")
	for i := len(p) - 1; i >= 0; i-- {
		if p[i] == '/' {
			return p[i+1:]
		}
	}
	return p
}
