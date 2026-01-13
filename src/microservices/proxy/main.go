package main

import (
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

func main() {
	port := envOrDefault("PORT", "8000")
	monolithURL := mustParseURL(envOrDefault("MONOLITH_URL", "http://localhost:8080"), "MONOLITH_URL")
	moviesURL := parseURL(envOrDefault("MOVIES_SERVICE_URL", ""))

	gradualMigration := strings.EqualFold(envOrDefault("GRADUAL_MIGRATION", "false"), "true")
	migrationPercent := clampInt(parseInt(envOrDefault("MOVIES_MIGRATION_PERCENT", "0"), 0), 0, 100)

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	monolithProxy := newReverseProxy(monolithURL)
	var moviesProxy *httputil.ReverseProxy
	if moviesURL != nil {
		moviesProxy = newReverseProxy(moviesURL)
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("Strangler Fig Proxy is healthy"))
	})

	// Handle movies-service health check
	mux.HandleFunc("/api/movies/health", func(w http.ResponseWriter, r *http.Request) {
		if moviesProxy != nil {
			log.Printf("proxy request %s %s -> movies-service health", r.Method, r.URL.Path)
			moviesProxy.ServeHTTP(w, r)
		} else {
			log.Printf("proxy request %s %s -> monolith (no movies-service)", r.Method, r.URL.Path)
			monolithProxy.ServeHTTP(w, r)
		}
	})

	mux.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		backendName := "monolith"
		proxy := monolithProxy

		if gradualMigration && moviesProxy != nil {
			if migrationPercent >= 100 {
				backendName = "movies-service"
				proxy = moviesProxy
			} else if migrationPercent <= 0 {
				// keep monolith
			} else {
				if rng.Intn(100) < migrationPercent {
					backendName = "movies-service"
					proxy = moviesProxy
				}
			}
		}

		log.Printf("proxy request %s %s -> %s (gradual=%t percent=%d)", r.Method, r.URL.Path, backendName, gradualMigration, migrationPercent)
		proxy.ServeHTTP(w, r)
	})

	mux.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		monolithProxy.ServeHTTP(w, r)
	})

	// Default behavior: proxy everything under /api to monolith.
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		monolithProxy.ServeHTTP(w, r)
	})

	log.Printf("Starting proxy service on port %s", port)
	log.Printf("MONOLITH_URL=%s", monolithURL.String())
	if moviesURL != nil {
		log.Printf("MOVIES_SERVICE_URL=%s", moviesURL.String())
	}
	log.Printf("GRADUAL_MIGRATION=%t MOVIES_MIGRATION_PERCENT=%d", gradualMigration, migrationPercent)

	server := &http.Server{
		Addr:              ":" + port,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	log.Fatal(server.ListenAndServe())
}

func newReverseProxy(target *url.URL) *httputil.ReverseProxy {
	p := httputil.NewSingleHostReverseProxy(target)
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("reverse proxy error: %v", err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
	}
	return p
}

func envOrDefault(key, def string) string {
	v := strings.TrimSpace(os.Getenv(key))
	if v == "" {
		return def
	}
	return v
}

func parseInt(s string, def int) int {
	v, err := strconv.Atoi(strings.TrimSpace(s))
	if err != nil {
		return def
	}
	return v
}

func clampInt(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func parseURL(raw string) *url.URL {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil
	}
	return u
}

func mustParseURL(raw, envKey string) *url.URL {
	u, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		log.Fatalf("invalid %s: %v", envKey, err)
	}
	return u
}
