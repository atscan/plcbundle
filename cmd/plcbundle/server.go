package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"git.urbach.dev/go/web"
	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"

	"tangled.org/atscan.net/plcbundle/bundle"
	"tangled.org/atscan.net/plcbundle/plc"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

var serverStartTime time.Time
var syncInterval time.Duration
var verboseMode bool
var resolverEnabled bool

func newServerHandler(mgr *bundle.Manager, syncMode bool, wsEnabled bool, resolverEnabled bool) web.Server {
	s := web.NewServer()

	// CORS middleware
	s.Use(corsMiddleware)

	// Root endpoint
	s.Get("/", func(ctx web.Context) error {
		return handleRoot(ctx, mgr, syncMode, wsEnabled, resolverEnabled)
	})

	// Bundle endpoints
	s.Get("/index.json", func(ctx web.Context) error {
		return handleIndexJSON(ctx, mgr)
	})

	s.Get("/bundle/:number", func(ctx web.Context) error {
		return handleBundle(ctx, mgr)
	})

	s.Get("/data/:number", func(ctx web.Context) error {
		return handleBundleData(ctx, mgr)
	})

	s.Get("/jsonl/:number", func(ctx web.Context) error {
		return handleBundleJSONL(ctx, mgr)
	})

	s.Get("/status", func(ctx web.Context) error {
		return handleStatus(ctx, mgr, syncMode, wsEnabled)
	})

	s.Get("/debug/memory", func(ctx web.Context) error {
		return handleDebugMemory(ctx, mgr)
	})

	// WebSocket endpoint - needs special handling
	if wsEnabled {
		s.Get("/ws", func(ctx web.Context) error {
			// WebSocket needs raw ResponseWriter, get it from underlying request
			handleWebSocketRaw(ctx, mgr)
			return nil
		})
	}

	// Sync mode endpoints
	if syncMode {
		s.Get("/mempool", func(ctx web.Context) error {
			return handleMempool(ctx, mgr)
		})
	}

	// DID resolution endpoints (must be LAST to avoid conflicts)
	if resolverEnabled {
		// Single catch-all handler for DID routes
		s.Get("/*path", func(ctx web.Context) error {
			path := ctx.Request().Param("path")

			// Remove leading slash
			path = strings.TrimPrefix(path, "/")

			// Parse DID and sub-path
			parts := strings.SplitN(path, "/", 2)
			did := parts[0]

			// Validate it's a DID
			if !strings.HasPrefix(did, "did:plc:") {
				return sendJSON(ctx, 404, map[string]string{"error": "not found"})
			}

			// Route based on sub-path
			if len(parts) == 1 {
				// /did:plc:xxx -> DID document
				return handleDIDDocumentLatest(ctx, mgr, did)
			} else if parts[1] == "data" {
				// /did:plc:xxx/data -> PLC state
				return handleDIDData(ctx, mgr, did)
			} else if parts[1] == "log/audit" {
				// /did:plc:xxx/log/audit -> Audit log
				return handleDIDAuditLog(ctx, mgr, did)
			}

			return sendJSON(ctx, 404, map[string]string{"error": "not found"})
		})
	}

	return s
}

// Helper to send JSON responses using goccy/go-json
func sendJSON(ctx web.Context, statusCode int, data interface{}) error {
	ctx.Response().SetHeader("Content-Type", "application/json")

	if statusCode != 200 {
		ctx.Status(statusCode)
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	_, err = ctx.Response().Write(jsonData)
	return err
}

// CORS middleware
func corsMiddleware(ctx web.Context) error {
	ctx.Response().SetHeader("Access-Control-Allow-Origin", "*")
	ctx.Response().SetHeader("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")

	if requestedHeaders := ctx.Request().Header("Access-Control-Request-Headers"); requestedHeaders != "" {
		ctx.Response().SetHeader("Access-Control-Allow-Headers", requestedHeaders)
	} else {
		ctx.Response().SetHeader("Access-Control-Allow-Headers", "*")
	}

	ctx.Response().SetHeader("Access-Control-Max-Age", "86400")

	if ctx.Request().Method() == "OPTIONS" {
		return ctx.Status(204).String("")
	}

	return ctx.Next(ctx)
}

func handleRoot(ctx web.Context, mgr *bundle.Manager, syncMode bool, wsEnabled bool, resolverEnabled bool) error {
	ctx.Response().SetHeader("Content-Type", "text/plain; charset=utf-8")

	index := mgr.GetIndex()
	stats := index.GetStats()
	bundleCount := stats["bundle_count"].(int)

	baseURL := getBaseURLFromContext(ctx)
	wsURL := getWSURLFromContext(ctx)

	var sb strings.Builder

	sb.WriteString(`

	⠀⠀⠀⠀⠀⠀⠀⠀⠀⠄⠀⡀⠀⠀⠀⠀⠀⠀⢀⠀⠀⡀⠀⢀⠀⢀⡀⣤⡢⣤⡤⡀⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⡄⡄⠐⡀⠈⣀⠀⡠⡠⠀⣢⣆⢌⡾⢙⠺⢽⠾⡋⣻⡷⡫⢵⣭⢦⣴⠦⠀⢠⠀⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢠⣤⣽⣥⡈⠧⣂⢧⢾⠕⠞⠡⠊⠁⣐⠉⠀⠉⢍⠀⠉⠌⡉⠀⠂⠁⠱⠉⠁⢝⠻⠎⣬⢌⡌⣬⣡⣀⣢⣄⡄⠀⡀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⢸⣿⣿⢿⣾⣯⣑⢄⡂⠀⠄⠂⠀⠀⢀⠀⠀⠐⠀⠀⠀⠀⠀⠀⠀⠀⠄⠐⠀⠀⠀⠀⣄⠭⠂⠈⠜⣩⣿⢝⠃⠀⠁⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢀⣻⡟⠏⠀⠚⠈⠚⡉⡝⢶⣱⢤⣅⠈⠀⠄⠀⠀⠀⠀⠀⠠⠀⠀⡂⠐⣤⢕⡪⢼⣈⡹⡇⠏⠏⠋⠅⢃⣪⡏⡇⡍⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠺⣻⡄⠀⠀⠀⢠⠌⠃⠐⠉⢡⠱⠧⠝⡯⣮⢶⣴⣤⡆⢐⣣⢅⣮⡟⠦⠍⠉⠀⠁⠐⠀⠀⠀⠄⠐⠡⣽⡸⣎⢁⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⢈⡻⣧⠀⠁⠐⠀⠀⠀⠀⠀⠀⠊⠀⠕⢀⡉⠈⡫⠽⡿⡟⠿⠟⠁⠀⠀⠄⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⠬⠥⣋⡯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⡀⣾⡍⠕⡀⠀⠀⠀⠄⠠⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠥⣤⢌⠀⠀⠀⠀⠀⠀⠀⠀⠀⠁⠀⠀⠄⢀⠀⢝⢞⣫⡆⡄⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣽⡶⡄⠐⡀⠀⠀⠀⠀⠀⠀⢀⠀⠄⠀⠀⠀⠄⠁⠇⣷⡆⠀⠀⠀⢀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⡸⢝⣮⠍⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⢀⠀⢾⣷⠀⠠⡀⠀⠀⠀⠀⢀⠀⠀⠀⠀⠀⠁⡁⠀⠀⣾⡥⠖⠀⠀⠀⠂⠀⠀⠀⠀⠀⠁⠀⡀⠁⠀⠀⠻⢳⣻⢄⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣞⡙⠨⣀⠠⠄⠀⠂⠀⠀⠀⠈⢀⠀⠀⠀⠀⠀⠤⢚⢢⣟⠀⠀⠀⠀⡐⠀⠀⡀⠀⠀⠀⠀⠁⠈⠌⠊⣯⣮⡏⠡⠂⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⣻⡟⡄⡡⣄⠀⠠⠀⠀⡅⠀⠐⠀⡀⠀⡀⠀⠄⠈⠃⠳⠪⠤⠀⠀⠀⠀⡀⠀⠂⠀⠀⠀⠁⠈⢠⣠⠒⠻⣻⡧⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠪⡎⠠⢌⠑⡀⠂⠀⠄⠠⠀⠠⠀⠁⡀⠠⠠⡀⣀⠜⢏⡅⠀⠀⡀⠁⠀⠀⠁⠁⠐⠄⡀⢀⠂⠀⠄⢑⣿⣿⣿⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠼⣻⠧⣣⣀⠐⠨⠁⠕⢈⢀⢀⡁⠀⠈⠠⢀⠀⠐⠜⣽⡗⡤⠀⠂⠀⠠⠀⢂⠠⠀⠁⠄⠀⠔⠀⠑⣨⣿⢯⠋⡅⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⡚⣷⣭⠎⢃⡗⠄⡄⢀⠁⠀⠅⢀⢅⡀⠠⠀⢠⡀⡩⠷⢇⠀⡀⠄⡠⠤⠆⣀⡀⠄⠉⣠⠃⠴⠀⠈⢁⣿⡛⡯⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠘⡬⡿⣿⡏⡻⡯⠌⢁⢛⠠⠓⠐⠐⠐⠌⠃⠋⠂⡢⢰⣈⢏⣰⠂⠈⠀⠠⠒⠡⠌⠫⠭⠩⠢⡬⠆⠿⢷⢿⡽⡧⠉⠊⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠺⣷⣺⣗⣿⡶⡎⡅⣣⢎⠠⡅⣢⡖⠴⠬⡈⠂⡨⢡⠾⣣⣢⠀⠀⡹⠄⡄⠄⡇⣰⡖⡊⠔⢹⣄⣿⣭⣵⣿⢷⡀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠩⣿⣿⣲⣿⣷⣟⣼⠟⣬⢉⡠⣪⢜⣂⣁⠥⠓⠚⡁⢶⣷⣠⠂⡄⡢⣀⡐⠧⢆⣒⡲⡳⡫⢟⡃⢪⡧⣟⡟⣯⠐⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⢺⠟⢿⢟⢻⡗⡮⡿⣲⢷⣆⣏⣇⡧⣄⢖⠾⡷⣿⣤⢳⢷⣣⣦⡜⠗⣭⢂⠩⣹⢿⡲⢎⡧⣕⣖⣓⣽⡿⡖⡿⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠈⠉⠂⠂⠏⠿⢻⣥⡪⢽⣳⣳⣥⡶⣫⣍⢐⣥⣻⣾⡻⣅⢭⡴⢭⣿⠕⣧⡭⣞⣻⣣⣻⢿⠟⠛⠙⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠄⠋⠫⠯⣍⢻⣿⣿⣷⣕⣵⣹⣽⣿⣷⣇⡏⣿⡿⣍⡝⠵⠯⠁⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠐⠠⠁⠋⢣⠓⡍⣫⠹⣿⣿⣷⡿⠯⠺⠁⠁⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀
⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠋⢀⠋⢈⡿⠿⠁⠉⠁⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀

                        plcbundle server

*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
| ⚠️ Preview Version – Do Not Use In Production!                 |
*~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~*
| This project and plcbundle specification is currently          |
| unstable and under heavy development. Things can break at      |
| any time. Do not use this for production systems.              |
| Please wait for the 1.0 release.                               |
|________________________________________________________________|

`)

	sb.WriteString("\nplcbundle server\n\n")
	sb.WriteString("What is PLC Bundle?\n")
	sb.WriteString("━━━━━━━━━━━━━━━━━━━━\n")
	sb.WriteString("plcbundle archives AT Protocol's DID PLC Directory operations into\n")
	sb.WriteString("immutable, cryptographically-chained bundles of 10,000 operations.\n\n")
	sb.WriteString("More info: https://tangled.org/@atscan.net/plcbundle\n\n")

	if bundleCount > 0 {
		sb.WriteString("Bundles\n")
		sb.WriteString("━━━━━━━\n")
		sb.WriteString(fmt.Sprintf("  Bundle count:  %d\n", bundleCount))

		firstBundle := stats["first_bundle"].(int)
		lastBundle := stats["last_bundle"].(int)
		totalSize := stats["total_size"].(int64)
		totalUncompressed := stats["total_uncompressed_size"].(int64)

		sb.WriteString(fmt.Sprintf("  Last bundle:   %d (%s)\n", lastBundle,
			stats["updated_at"].(time.Time).Format("2006-01-02 15:04:05")))
		sb.WriteString(fmt.Sprintf("  Range:         %06d - %06d\n", firstBundle, lastBundle))
		sb.WriteString(fmt.Sprintf("  Total size:    %.2f MB\n", float64(totalSize)/(1000*1000)))
		sb.WriteString(fmt.Sprintf("  Uncompressed:  %.2f MB (%.2fx)\n",
			float64(totalUncompressed)/(1000*1000),
			float64(totalUncompressed)/float64(totalSize)))

		if gaps, ok := stats["gaps"].(int); ok && gaps > 0 {
			sb.WriteString(fmt.Sprintf("  ⚠ Gaps:        %d missing bundles\n", gaps))
		}

		firstMeta, err := index.GetBundle(firstBundle)
		if err == nil {
			sb.WriteString(fmt.Sprintf("\n  Root: %s\n", firstMeta.Hash))
		}

		lastMeta, err := index.GetBundle(lastBundle)
		if err == nil {
			sb.WriteString(fmt.Sprintf("  Head: %s\n", lastMeta.Hash))
		}
	}

	if syncMode {
		mempoolStats := mgr.GetMempoolStats()
		count := mempoolStats["count"].(int)
		targetBundle := mempoolStats["target_bundle"].(int)
		canCreate := mempoolStats["can_create_bundle"].(bool)

		sb.WriteString("\nMempool Stats\n")
		sb.WriteString("━━━━━━━━━━━━━\n")
		sb.WriteString(fmt.Sprintf("  Target bundle:     %d\n", targetBundle))
		sb.WriteString(fmt.Sprintf("  Operations:        %d / %d\n", count, bundle.BUNDLE_SIZE))
		sb.WriteString(fmt.Sprintf("  Can create bundle: %v\n", canCreate))

		if count > 0 {
			progress := float64(count) / float64(bundle.BUNDLE_SIZE) * 100
			sb.WriteString(fmt.Sprintf("  Progress:          %.1f%%\n", progress))

			barWidth := 50
			filled := int(float64(barWidth) * float64(count) / float64(bundle.BUNDLE_SIZE))
			if filled > barWidth {
				filled = barWidth
			}
			bar := strings.Repeat("█", filled) + strings.Repeat("░", barWidth-filled)
			sb.WriteString(fmt.Sprintf("  [%s]\n", bar))

			if firstTime, ok := mempoolStats["first_time"].(time.Time); ok {
				sb.WriteString(fmt.Sprintf("  First op:          %s\n", firstTime.Format("2006-01-02 15:04:05")))
			}
			if lastTime, ok := mempoolStats["last_time"].(time.Time); ok {
				sb.WriteString(fmt.Sprintf("  Last op:           %s\n", lastTime.Format("2006-01-02 15:04:05")))
			}
		} else {
			sb.WriteString("  (empty)\n")
		}
	}

	if didStats := mgr.GetDIDIndexStats(); didStats["exists"].(bool) {
		sb.WriteString("\nDID Index\n")
		sb.WriteString("━━━━━━━━━\n")
		sb.WriteString("  Status:        enabled\n")

		indexedDIDs := didStats["indexed_dids"].(int64)
		mempoolDIDs := didStats["mempool_dids"].(int64)
		totalDIDs := didStats["total_dids"].(int64)

		if mempoolDIDs > 0 {
			sb.WriteString(fmt.Sprintf("  Total DIDs:    %s (%s indexed + %s mempool)\n",
				formatNumber(int(totalDIDs)),
				formatNumber(int(indexedDIDs)),
				formatNumber(int(mempoolDIDs))))
		} else {
			sb.WriteString(fmt.Sprintf("  Total DIDs:    %s\n", formatNumber(int(totalDIDs))))
		}

		sb.WriteString(fmt.Sprintf("  Cached shards: %d / %d\n",
			didStats["cached_shards"], didStats["cache_limit"]))
		sb.WriteString("\n")
	}

	sb.WriteString("Server Stats\n")
	sb.WriteString("━━━━━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("  Version:       %s\n", version))
	if origin := mgr.GetPLCOrigin(); origin != "" {
		sb.WriteString(fmt.Sprintf("  Origin:        %s\n", origin))
	}
	sb.WriteString(fmt.Sprintf("  Sync mode:     %v\n", syncMode))
	sb.WriteString(fmt.Sprintf("  WebSocket:     %v\n", wsEnabled))
	sb.WriteString(fmt.Sprintf("  Resolver:      %v\n", resolverEnabled))
	sb.WriteString(fmt.Sprintf("  Uptime:        %s\n", time.Since(serverStartTime).Round(time.Second)))

	sb.WriteString("\n\nAPI Endpoints\n")
	sb.WriteString("━━━━━━━━━━━━━\n")
	sb.WriteString("  GET  /                    This info page\n")
	sb.WriteString("  GET  /index.json          Full bundle index\n")
	sb.WriteString("  GET  /bundle/:number      Bundle metadata (JSON)\n")
	sb.WriteString("  GET  /data/:number        Raw bundle (zstd compressed)\n")
	sb.WriteString("  GET  /jsonl/:number       Decompressed JSONL stream\n")
	sb.WriteString("  GET  /status              Server status\n")
	sb.WriteString("  GET  /mempool             Mempool operations (JSONL)\n")

	if resolverEnabled {
		sb.WriteString("\nDID Resolution\n")
		sb.WriteString("━━━━━━━━━━━━━━\n")
		sb.WriteString("  GET  /:did                    DID Document (W3C format)\n")
		sb.WriteString("  GET  /:did/data               PLC State (raw format)\n")
		sb.WriteString("  GET  /:did/log/audit          Operation history\n")

		didStats := mgr.GetDIDIndexStats()
		if didStats["exists"].(bool) {
			sb.WriteString(fmt.Sprintf("\n  Index: %s DIDs indexed\n",
				formatNumber(int(didStats["total_dids"].(int64)))))
		} else {
			sb.WriteString("\n  ⚠️  Index: not built (will use slow scan)\n")
		}
		sb.WriteString("\n")
	}

	if wsEnabled {
		sb.WriteString("\nWebSocket Endpoints\n")
		sb.WriteString("━━━━━━━━━━━━━━━━━━━\n")
		sb.WriteString("  WS   /ws                      Live stream (new operations only)\n")
		sb.WriteString("  WS   /ws?cursor=0             Stream all from beginning\n")
		sb.WriteString("  WS   /ws?cursor=N             Stream from cursor N\n\n")
		sb.WriteString("Cursor Format:\n")
		sb.WriteString("  Global record number: (bundleNumber × 10,000) + position\n")
		sb.WriteString("  Example: 88410345 = bundle 8841, position 345\n")
		sb.WriteString("  Default: starts from latest (skips all historical data)\n")

		latestCursor := mgr.GetCurrentCursor()
		bundledOps := len(index.GetBundles()) * bundle.BUNDLE_SIZE
		mempoolOps := latestCursor - bundledOps

		if syncMode && mempoolOps > 0 {
			sb.WriteString(fmt.Sprintf("  Current latest: %d (%d bundled + %d mempool)\n",
				latestCursor, bundledOps, mempoolOps))
		} else {
			sb.WriteString(fmt.Sprintf("  Current latest: %d (%d bundles)\n",
				latestCursor, len(index.GetBundles())))
		}
	}

	sb.WriteString("\nExamples\n")
	sb.WriteString("━━━━━━━━\n")
	sb.WriteString(fmt.Sprintf("  curl %s/bundle/1\n", baseURL))
	sb.WriteString(fmt.Sprintf("  curl %s/data/42 -o 000042.jsonl.zst\n", baseURL))
	sb.WriteString(fmt.Sprintf("  curl %s/jsonl/1\n", baseURL))

	if wsEnabled {
		sb.WriteString(fmt.Sprintf("  websocat %s/ws\n", wsURL))
		sb.WriteString(fmt.Sprintf("  websocat '%s/ws?cursor=0'\n", wsURL))
	}

	if syncMode {
		sb.WriteString(fmt.Sprintf("  curl %s/status\n", baseURL))
		sb.WriteString(fmt.Sprintf("  curl %s/mempool\n", baseURL))
	}

	sb.WriteString("\n────────────────────────────────────────────────────────────────\n")
	sb.WriteString("https://tangled.org/@atscan.net/plcbundle\n")

	return ctx.String(sb.String())
}

func handleIndexJSON(ctx web.Context, mgr *bundle.Manager) error {
	index := mgr.GetIndex()
	return sendJSON(ctx, 200, index)
}

func handleBundle(ctx web.Context, mgr *bundle.Manager) error {
	bundleNum, err := strconv.Atoi(ctx.Request().Param("number"))
	if err != nil {
		return sendJSON(ctx, 400, map[string]string{"error": "Invalid bundle number"})
	}

	meta, err := mgr.GetIndex().GetBundle(bundleNum)
	if err != nil {
		return sendJSON(ctx, 404, map[string]string{"error": "Bundle not found"})
	}

	return sendJSON(ctx, 200, meta)
}

func handleBundleData(ctx web.Context, mgr *bundle.Manager) error {
	bundleNum, err := strconv.Atoi(ctx.Request().Param("number"))
	if err != nil {
		return sendJSON(ctx, 400, map[string]string{"error": "Invalid bundle number"})
	}

	reader, err := mgr.StreamBundleRaw(context.Background(), bundleNum)
	if err != nil {
		if strings.Contains(err.Error(), "not in index") || strings.Contains(err.Error(), "not found") {
			return sendJSON(ctx, 400, map[string]string{"error": "Bundle not found"})
		}
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}
	defer reader.Close()

	ctx.Response().SetHeader("Content-Type", "application/zstd")
	ctx.Response().SetHeader("Content-Disposition", fmt.Sprintf("attachment; filename=%06d.jsonl.zst", bundleNum))

	_, err = io.Copy(ctx.Response(), reader)
	return err
}

func handleBundleJSONL(ctx web.Context, mgr *bundle.Manager) error {
	bundleNum, err := strconv.Atoi(ctx.Request().Param("number"))
	if err != nil {
		return sendJSON(ctx, 400, map[string]string{"error": "Invalid bundle number"})
	}

	reader, err := mgr.StreamBundleDecompressed(context.Background(), bundleNum)
	if err != nil {
		if strings.Contains(err.Error(), "not in index") || strings.Contains(err.Error(), "not found") {
			return sendJSON(ctx, 404, map[string]string{"error": "Bundle not found"})
		}
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}
	defer reader.Close()

	ctx.Response().SetHeader("Content-Type", "application/x-ndjson")
	ctx.Response().SetHeader("Content-Disposition", fmt.Sprintf("attachment; filename=%06d.jsonl", bundleNum))

	_, err = io.Copy(ctx.Response(), reader)
	return err
}

func handleStatus(ctx web.Context, mgr *bundle.Manager, syncMode bool, wsEnabled bool) error {
	index := mgr.GetIndex()
	indexStats := index.GetStats()

	response := StatusResponse{
		Server: ServerStatus{
			Version:          version,
			UptimeSeconds:    int(time.Since(serverStartTime).Seconds()),
			SyncMode:         syncMode,
			WebSocketEnabled: wsEnabled,
			Origin:           mgr.GetPLCOrigin(),
		},
		Bundles: BundleStatus{
			Count:            indexStats["bundle_count"].(int),
			TotalSize:        indexStats["total_size"].(int64),
			UncompressedSize: indexStats["total_uncompressed_size"].(int64),
		},
	}

	if syncMode && syncInterval > 0 {
		response.Server.SyncIntervalSeconds = int(syncInterval.Seconds())
	}

	if bundleCount := response.Bundles.Count; bundleCount > 0 {
		firstBundle := indexStats["first_bundle"].(int)
		lastBundle := indexStats["last_bundle"].(int)

		response.Bundles.FirstBundle = firstBundle
		response.Bundles.LastBundle = lastBundle
		response.Bundles.StartTime = indexStats["start_time"].(time.Time)
		response.Bundles.EndTime = indexStats["end_time"].(time.Time)

		if firstMeta, err := index.GetBundle(firstBundle); err == nil {
			response.Bundles.RootHash = firstMeta.Hash
		}

		if lastMeta, err := index.GetBundle(lastBundle); err == nil {
			response.Bundles.HeadHash = lastMeta.Hash
			response.Bundles.HeadAgeSeconds = int(time.Since(lastMeta.EndTime).Seconds())
		}

		if gaps, ok := indexStats["gaps"].(int); ok {
			response.Bundles.Gaps = gaps
			response.Bundles.HasGaps = gaps > 0
			if gaps > 0 {
				response.Bundles.GapNumbers = index.FindGaps()
			}
		}

		totalOps := bundleCount * bundle.BUNDLE_SIZE
		response.Bundles.TotalOperations = totalOps

		duration := response.Bundles.EndTime.Sub(response.Bundles.StartTime)
		if duration.Hours() > 0 {
			response.Bundles.AvgOpsPerHour = int(float64(totalOps) / duration.Hours())
		}
	}

	if syncMode {
		mempoolStats := mgr.GetMempoolStats()

		if count, ok := mempoolStats["count"].(int); ok {
			mempool := &MempoolStatus{
				Count:            count,
				TargetBundle:     mempoolStats["target_bundle"].(int),
				CanCreateBundle:  mempoolStats["can_create_bundle"].(bool),
				MinTimestamp:     mempoolStats["min_timestamp"].(time.Time),
				Validated:        mempoolStats["validated"].(bool),
				ProgressPercent:  float64(count) / float64(bundle.BUNDLE_SIZE) * 100,
				BundleSize:       bundle.BUNDLE_SIZE,
				OperationsNeeded: bundle.BUNDLE_SIZE - count,
			}

			if firstTime, ok := mempoolStats["first_time"].(time.Time); ok {
				mempool.FirstTime = firstTime
				mempool.TimespanSeconds = int(time.Since(firstTime).Seconds())
			}
			if lastTime, ok := mempoolStats["last_time"].(time.Time); ok {
				mempool.LastTime = lastTime
				mempool.LastOpAgeSeconds = int(time.Since(lastTime).Seconds())
			}

			if count > 100 && count < bundle.BUNDLE_SIZE {
				if !mempool.FirstTime.IsZero() && !mempool.LastTime.IsZero() {
					timespan := mempool.LastTime.Sub(mempool.FirstTime)
					if timespan.Seconds() > 0 {
						opsPerSec := float64(count) / timespan.Seconds()
						remaining := bundle.BUNDLE_SIZE - count
						mempool.EtaNextBundleSeconds = int(float64(remaining) / opsPerSec)
					}
				}
			}

			response.Mempool = mempool
		}
	}

	return sendJSON(ctx, 200, response)
}

func handleMempool(ctx web.Context, mgr *bundle.Manager) error {
	ops, err := mgr.GetMempoolOperations()
	if err != nil {
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}

	ctx.Response().SetHeader("Content-Type", "application/x-ndjson")

	if len(ops) == 0 {
		return nil
	}

	for _, op := range ops {
		if len(op.RawJSON) > 0 {
			ctx.Response().Write(op.RawJSON)
		} else {
			data, _ := json.Marshal(op)
			ctx.Response().Write(data)
		}
		ctx.Response().Write([]byte("\n"))
	}

	return nil
}

func handleDebugMemory(ctx web.Context, mgr *bundle.Manager) error {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	didStats := mgr.GetDIDIndexStats()

	beforeAlloc := m.Alloc / 1024 / 1024

	runtime.GC()
	runtime.ReadMemStats(&m)
	afterAlloc := m.Alloc / 1024 / 1024

	return ctx.String(fmt.Sprintf(`Memory Stats:
  Alloc:      %d MB
  TotalAlloc: %d MB
  Sys:        %d MB
  NumGC:      %d

DID Index:
  Cached shards: %d/%d

After GC:
  Alloc:      %d MB
`,
		beforeAlloc,
		m.TotalAlloc/1024/1024,
		m.Sys/1024/1024,
		m.NumGC,
		didStats["cached_shards"],
		didStats["cache_limit"],
		afterAlloc))
}

func handleDIDDocumentLatest(ctx web.Context, mgr *bundle.Manager, did string) error {
	op, err := mgr.GetLatestDIDOperation(context.Background(), did)
	if err != nil {
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}

	doc, err := plc.ResolveDIDDocument(did, []plc.PLCOperation{*op})
	if err != nil {
		if strings.Contains(err.Error(), "deactivated") {
			return sendJSON(ctx, 410, map[string]string{"error": "DID has been deactivated"})
		}
		return sendJSON(ctx, 500, map[string]string{"error": fmt.Sprintf("Resolution failed: %v", err)})
	}
	ctx.Response().SetHeader("Content-Type", "application/did+ld+json")
	return sendJSON(ctx, 200, doc)
}

func handleDIDData(ctx web.Context, mgr *bundle.Manager, did string) error {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return sendJSON(ctx, 400, map[string]string{"error": "Invalid DID format"})
	}

	operations, err := mgr.GetDIDOperations(context.Background(), did, false)
	if err != nil {
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}

	if len(operations) == 0 {
		return sendJSON(ctx, 404, map[string]string{"error": "DID not found"})
	}

	state, err := plc.BuildDIDState(did, operations)
	if err != nil {
		if strings.Contains(err.Error(), "deactivated") {
			return sendJSON(ctx, 410, map[string]string{"error": "DID has been deactivated"})
		}
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}

	return sendJSON(ctx, 200, state)
}

func handleDIDAuditLog(ctx web.Context, mgr *bundle.Manager, did string) error {
	if err := plc.ValidateDIDFormat(did); err != nil {
		return sendJSON(ctx, 400, map[string]string{"error": "Invalid DID format"})
	}

	operations, err := mgr.GetDIDOperations(context.Background(), did, false)
	if err != nil {
		return sendJSON(ctx, 500, map[string]string{"error": err.Error()})
	}

	if len(operations) == 0 {
		return sendJSON(ctx, 404, map[string]string{"error": "DID not found"})
	}

	auditLog := plc.FormatAuditLog(operations)
	return sendJSON(ctx, 200, auditLog)
}

// WebSocket handler wrapper for web framework
func handleWebSocketRaw(ctx web.Context, mgr *bundle.Manager) {
	// The web framework doesn't expose ResponseWriter directly
	// We need to use reflection or type assertion to get it
	// For now, we'll implement a workaround by getting the underlying HTTP objects

	cursorStr := ctx.Request().Query().Param("cursor")
	var cursor int

	if cursorStr == "" {
		cursor = mgr.GetCurrentCursor()
	} else {
		var err error
		cursor, err = strconv.Atoi(cursorStr)
		if err != nil || cursor < 0 {
			ctx.Status(400).String("Invalid cursor: must be non-negative integer")
			return
		}
	}

	// Access underlying ResponseWriter through interface assertion
	type ResponseWriterGetter interface {
		ResponseWriter() http.ResponseWriter
	}

	type RequestGetter interface {
		HTTPRequest() *http.Request
	}

	var w http.ResponseWriter
	var r *http.Request

	// Try to get ResponseWriter (framework-specific)
	if rwg, ok := ctx.(ResponseWriterGetter); ok {
		w = rwg.ResponseWriter()
	}

	if rg, ok := ctx.(RequestGetter); ok {
		r = rg.HTTPRequest()
	}

	// If we can't get them, we need to upgrade manually
	// This is a limitation - WebSocket needs direct access
	if w == nil || r == nil {
		ctx.Status(500).String("WebSocket not supported")
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "WebSocket upgrade failed: %v\n", err)
		return
	}
	defer conn.Close()

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				if websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
					fmt.Fprintf(os.Stderr, "WebSocket: client closed connection\n")
				}
				return
			}
		}
	}()

	bgCtx := context.Background()

	if err := streamLive(bgCtx, conn, mgr, cursor, done); err != nil {
		fmt.Fprintf(os.Stderr, "WebSocket stream error: %v\n", err)
	}
}

// streamLive and other WebSocket functions remain unchanged
func streamLive(ctx context.Context, conn *websocket.Conn, mgr *bundle.Manager, startCursor int, done chan struct{}) error {
	index := mgr.GetIndex()
	bundles := index.GetBundles()
	currentRecord := startCursor

	if len(bundles) > 0 {
		startBundleIdx := startCursor / bundle.BUNDLE_SIZE
		startPosition := startCursor % bundle.BUNDLE_SIZE

		if startBundleIdx < len(bundles) {
			for i := startBundleIdx; i < len(bundles); i++ {
				skipUntil := 0
				if i == startBundleIdx {
					skipUntil = startPosition
				}

				newRecordCount, err := streamBundle(ctx, conn, mgr, bundles[i].BundleNumber, skipUntil, done)
				if err != nil {
					return err
				}
				currentRecord += newRecordCount
			}
		}
	}

	lastSeenMempoolCount := 0
	if err := streamMempool(conn, mgr, startCursor, len(bundles)*bundle.BUNDLE_SIZE, &currentRecord, &lastSeenMempoolCount, done); err != nil {
		return err
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastBundleCount := len(bundles)
	if verboseMode {
		fmt.Fprintf(os.Stderr, "WebSocket: entering live mode at cursor %d\n", currentRecord)
	}

	for {
		select {
		case <-done:
			if verboseMode {
				fmt.Fprintf(os.Stderr, "WebSocket: client disconnected, stopping stream\n")
			}
			return nil

		case <-ticker.C:
			index = mgr.GetIndex()
			bundles = index.GetBundles()

			if len(bundles) > lastBundleCount {
				newBundleCount := len(bundles) - lastBundleCount

				if verboseMode {
					fmt.Fprintf(os.Stderr, "WebSocket: %d new bundle(s) created (operations already streamed from mempool)\n", newBundleCount)
				}

				currentRecord += newBundleCount * bundle.BUNDLE_SIZE
				lastBundleCount = len(bundles)
				lastSeenMempoolCount = 0
			}

			if err := streamMempool(conn, mgr, startCursor, len(bundles)*bundle.BUNDLE_SIZE, &currentRecord, &lastSeenMempoolCount, done); err != nil {
				return err
			}

			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return err
			}
		}
	}
}

func streamBundle(ctx context.Context, conn *websocket.Conn, mgr *bundle.Manager, bundleNumber int, skipUntil int, done chan struct{}) (int, error) {
	reader, err := mgr.StreamBundleDecompressed(ctx, bundleNumber)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to stream bundle %d: %v\n", bundleNumber, err)
		return 0, nil
	}
	defer reader.Close()

	scanner := bufio.NewScanner(reader)
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 1024*1024)

	position := 0
	streamed := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		if position < skipUntil {
			position++
			continue
		}

		select {
		case <-done:
			return streamed, nil
		default:
		}

		if err := conn.WriteMessage(websocket.TextMessage, line); err != nil {
			return streamed, err
		}

		position++
		streamed++

		if streamed%1000 == 0 {
			conn.WriteMessage(websocket.PingMessage, nil)
		}
	}

	if err := scanner.Err(); err != nil {
		return streamed, fmt.Errorf("scanner error on bundle %d: %w", bundleNumber, err)
	}

	return streamed, nil
}

func streamMempool(conn *websocket.Conn, mgr *bundle.Manager, startCursor int, bundleRecordBase int, currentRecord *int, lastSeenCount *int, done chan struct{}) error {
	mempoolOps, err := mgr.GetMempoolOperations()
	if err != nil {
		return nil
	}

	if len(mempoolOps) <= *lastSeenCount {
		return nil
	}

	newOps := len(mempoolOps) - *lastSeenCount
	if newOps > 0 && verboseMode {
		fmt.Fprintf(os.Stderr, "WebSocket: streaming %d new mempool operation(s)\n", newOps)
	}

	for i := *lastSeenCount; i < len(mempoolOps); i++ {
		recordNum := bundleRecordBase + i
		if recordNum < startCursor {
			continue
		}

		select {
		case <-done:
			return nil
		default:
		}

		if err := sendOperation(conn, mempoolOps[i]); err != nil {
			return err
		}
		*currentRecord++
	}

	*lastSeenCount = len(mempoolOps)
	return nil
}

func sendOperation(conn *websocket.Conn, op plc.PLCOperation) error {
	var data []byte
	var err error

	if len(op.RawJSON) > 0 {
		data = op.RawJSON
	} else {
		data, err = json.Marshal(op)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to marshal operation: %v\n", err)
			return nil
		}
	}

	if err := conn.WriteMessage(websocket.TextMessage, data); err != nil {
		if !websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
			fmt.Fprintf(os.Stderr, "WebSocket write error: %v\n", err)
		}
		return err
	}

	return nil
}

// Helper functions

func getScheme(r *http.Request) string {
	if r.TLS != nil {
		return "https"
	}

	if proto := r.Header.Get("X-Forwarded-Proto"); proto != "" {
		return proto
	}

	if r.Header.Get("X-Forwarded-Ssl") == "on" {
		return "https"
	}

	return "http"
}

func getWSScheme(r *http.Request) string {
	if getScheme(r) == "https" {
		return "wss"
	}
	return "ws"
}

func getBaseURLFromContext(ctx web.Context) string {
	// Get host from request
	host := ctx.Request().Header("Host")
	// Assume http since we're behind reverse proxy
	return fmt.Sprintf("http://%s", host)
}

func getWSURLFromContext(ctx web.Context) string {
	host := ctx.Request().Header("Host")
	return fmt.Sprintf("ws://%s", host)
}

// Response types

type StatusResponse struct {
	Bundles BundleStatus   `json:"bundles"`
	Mempool *MempoolStatus `json:"mempool,omitempty"`
	Server  ServerStatus   `json:"server"`
}

type ServerStatus struct {
	Version             string `json:"version"`
	UptimeSeconds       int    `json:"uptime_seconds"`
	SyncMode            bool   `json:"sync_mode"`
	SyncIntervalSeconds int    `json:"sync_interval_seconds,omitempty"`
	WebSocketEnabled    bool   `json:"websocket_enabled"`
	Origin              string `json:"origin,omitempty"`
}

type BundleStatus struct {
	Count            int       `json:"count"`
	FirstBundle      int       `json:"first_bundle,omitempty"`
	LastBundle       int       `json:"last_bundle,omitempty"`
	TotalSize        int64     `json:"total_size"`
	UncompressedSize int64     `json:"uncompressed_size,omitempty"`
	CompressionRatio float64   `json:"compression_ratio,omitempty"`
	TotalOperations  int       `json:"total_operations,omitempty"`
	AvgOpsPerHour    int       `json:"avg_ops_per_hour,omitempty"`
	StartTime        time.Time `json:"start_time,omitempty"`
	EndTime          time.Time `json:"end_time,omitempty"`
	UpdatedAt        time.Time `json:"updated_at"`
	HeadAgeSeconds   int       `json:"head_age_seconds,omitempty"`
	RootHash         string    `json:"root_hash,omitempty"`
	HeadHash         string    `json:"head_hash,omitempty"`
	Gaps             int       `json:"gaps,omitempty"`
	HasGaps          bool      `json:"has_gaps"`
	GapNumbers       []int     `json:"gap_numbers,omitempty"`
}

type MempoolStatus struct {
	Count                int       `json:"count"`
	TargetBundle         int       `json:"target_bundle"`
	CanCreateBundle      bool      `json:"can_create_bundle"`
	MinTimestamp         time.Time `json:"min_timestamp"`
	Validated            bool      `json:"validated"`
	ProgressPercent      float64   `json:"progress_percent"`
	BundleSize           int       `json:"bundle_size"`
	OperationsNeeded     int       `json:"operations_needed"`
	FirstTime            time.Time `json:"first_time,omitempty"`
	LastTime             time.Time `json:"last_time,omitempty"`
	TimespanSeconds      int       `json:"timespan_seconds,omitempty"`
	LastOpAgeSeconds     int       `json:"last_op_age_seconds,omitempty"`
	EtaNextBundleSeconds int       `json:"eta_next_bundle_seconds,omitempty"`
}

// Background sync

func runSync(ctx context.Context, mgr *bundle.Manager, interval time.Duration, verbose bool, resolverEnabled bool) {
	syncBundles(ctx, mgr, verbose, resolverEnabled)

	fmt.Fprintf(os.Stderr, "[Sync] Starting sync loop (interval: %s)\n", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	saveTicker := time.NewTicker(5 * time.Minute)
	defer saveTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			if err := mgr.SaveMempool(); err != nil {
				fmt.Fprintf(os.Stderr, "[Sync] Failed to save mempool: %v\n", err)
			}
			fmt.Fprintf(os.Stderr, "[Sync] Stopped\n")
			return

		case <-ticker.C:
			syncBundles(ctx, mgr, verbose, resolverEnabled)

		case <-saveTicker.C:
			stats := mgr.GetMempoolStats()
			if stats["count"].(int) > 0 && verbose {
				fmt.Fprintf(os.Stderr, "[Sync] Saving mempool (%d ops)\n", stats["count"])
				mgr.SaveMempool()
			}
		}
	}
}

func syncBundles(ctx context.Context, mgr *bundle.Manager, verbose bool, resolverEnabled bool) {
	cycleStart := time.Now()

	index := mgr.GetIndex()
	lastBundle := index.GetLastBundle()
	startBundle := 1
	if lastBundle != nil {
		startBundle = lastBundle.BundleNumber + 1
	}

	isInitialSync := (lastBundle == nil || lastBundle.BundleNumber < 10)

	if isInitialSync && !verbose {
		fmt.Fprintf(os.Stderr, "[Sync] Initial sync - fast loading mode (bundle %06d → ...)\n", startBundle)
	} else if verbose {
		fmt.Fprintf(os.Stderr, "[Sync] Checking for new bundles (current: %06d)...\n", startBundle-1)
	}

	mempoolBefore := mgr.GetMempoolStats()["count"].(int)
	fetchedCount := 0
	consecutiveErrors := 0

	for {
		currentBundle := startBundle + fetchedCount

		b, err := mgr.FetchNextBundle(ctx, !verbose)
		if err != nil {
			if isEndOfDataError(err) {
				mempoolAfter := mgr.GetMempoolStats()["count"].(int)
				addedOps := mempoolAfter - mempoolBefore
				duration := time.Since(cycleStart)

				if fetchedCount > 0 {
					fmt.Fprintf(os.Stderr, "[Sync] ✓ Bundle %06d | Synced: %d | Mempool: %d (+%d) | %dms\n",
						currentBundle-1, fetchedCount, mempoolAfter, addedOps, duration.Milliseconds())
				} else if !isInitialSync {
					fmt.Fprintf(os.Stderr, "[Sync] ✓ Bundle %06d | Up to date | Mempool: %d (+%d) | %dms\n",
						startBundle-1, mempoolAfter, addedOps, duration.Milliseconds())
				}
				break
			}

			consecutiveErrors++
			if verbose {
				fmt.Fprintf(os.Stderr, "[Sync] Error fetching bundle %06d: %v\n", currentBundle, err)
			}

			if consecutiveErrors >= 3 {
				fmt.Fprintf(os.Stderr, "[Sync] Too many errors, stopping\n")
				break
			}

			time.Sleep(5 * time.Second)
			continue
		}

		consecutiveErrors = 0

		if err := mgr.SaveBundle(ctx, b, !verbose); err != nil {
			fmt.Fprintf(os.Stderr, "[Sync] Error saving bundle %06d: %v\n", b.BundleNumber, err)
			break
		}

		fetchedCount++

		if !verbose {
			fmt.Fprintf(os.Stderr, "[Sync] ✓ %06d | hash=%s | content=%s | %d ops, %d DIDs\n",
				b.BundleNumber,
				b.Hash[:16]+"...",
				b.ContentHash[:16]+"...",
				len(b.Operations),
				b.DIDCount)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func isEndOfDataError(err error) bool {
	if err == nil {
		return false
	}

	errMsg := err.Error()
	return strings.Contains(errMsg, "insufficient operations") ||
		strings.Contains(errMsg, "no more operations available") ||
		strings.Contains(errMsg, "reached latest data")
}
