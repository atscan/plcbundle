package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

func (s *Server) handleRoot() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")

		index := s.manager.GetIndex()
		stats := index.GetStats()
		bundleCount := stats["bundle_count"].(int)

		baseURL := getBaseURL(r)
		wsURL := getWSURL(r)

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

		origin := s.manager.GetPLCOrigin()

		if bundleCount > 0 {
			sb.WriteString("Bundles\n")
			sb.WriteString("━━━━━━━\n")
			sb.WriteString(fmt.Sprintf("  Origin:        %s\n", origin))
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

		if s.config.SyncMode {
			mempoolStats := s.manager.GetMempoolStats()
			count := mempoolStats["count"].(int)
			targetBundle := mempoolStats["target_bundle"].(int)

			sb.WriteString("\nMempool\n")
			sb.WriteString("━━━━━━━\n")
			sb.WriteString(fmt.Sprintf("  Target bundle:     %d\n", targetBundle))
			sb.WriteString(fmt.Sprintf("  Operations:        %d / %d\n", count, types.BUNDLE_SIZE))

			if count > 0 {
				progress := float64(count) / float64(types.BUNDLE_SIZE) * 100
				sb.WriteString(fmt.Sprintf("  Progress:          %.1f%%\n", progress))

				barWidth := 50
				filled := int(float64(barWidth) * float64(count) / float64(types.BUNDLE_SIZE))
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

		if s.config.EnableResolver {

			sb.WriteString("\nResolver\n")
			sb.WriteString("━━━━━━━━\n")
			sb.WriteString("  Status:        enabled\n")

			if didStats := s.manager.GetDIDIndexStats(); didStats["exists"].(bool) {
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
			}
			sb.WriteString("\n")
		}

		sb.WriteString("Server Stats\n")
		sb.WriteString("━━━━━━━━━━━━\n")
		sb.WriteString(fmt.Sprintf("  Version:           %s\n", s.config.Version))
		sb.WriteString(fmt.Sprintf("  Sync mode:         %v\n", s.config.SyncMode))
		sb.WriteString(fmt.Sprintf("  WebSocket:         %v\n", s.config.EnableWebSocket))
		sb.WriteString(fmt.Sprintf("  Handle Resolver:   %v\n", s.manager.GetHandleResolver().GetBaseURL()))
		sb.WriteString(fmt.Sprintf("  Uptime:            %s\n", time.Since(s.startTime).Round(time.Second)))

		sb.WriteString("\n\nAPI Endpoints\n")
		sb.WriteString("━━━━━━━━━━━━━\n")
		sb.WriteString("  GET  /                    This info page\n")
		sb.WriteString("  GET  /index.json          Full bundle index\n")
		sb.WriteString("  GET  /bundle/:number      Bundle metadata (JSON)\n")
		sb.WriteString("  GET  /data/:number        Raw bundle (zstd compressed)\n")
		sb.WriteString("  GET  /jsonl/:number       Decompressed JSONL stream\n")
		sb.WriteString("  GET  /op/:pointer         Get single operation\n")
		sb.WriteString("  GET  /status              Server status\n")
		sb.WriteString("  GET  /mempool             Mempool operations (JSONL)\n")

		if s.config.EnableResolver {
			sb.WriteString("\nDID Resolution\n")
			sb.WriteString("━━━━━━━━━━━━━━\n")
			sb.WriteString("  GET  /:did                    DID Document (W3C format)\n")
			sb.WriteString("  GET  /:did/data               PLC State (raw format)\n")
			sb.WriteString("  GET  /:did/log/audit          Operation history\n")
		}

		if s.config.EnableWebSocket {
			sb.WriteString("\nWebSocket Endpoints\n")
			sb.WriteString("━━━━━━━━━━━━━━━━━━━\n")
			sb.WriteString("  WS   /ws                      Live stream (new operations only)\n")
			sb.WriteString("  WS   /ws?cursor=0             Stream all from beginning\n")
			sb.WriteString("  WS   /ws?cursor=N             Stream from cursor N\n\n")
			sb.WriteString("Cursor Format:\n")
			sb.WriteString("  Global record number: (bundleNumber × 10,000) + position\n")
			sb.WriteString("  Example: 88410345 = bundle 8841, position 345\n")
			sb.WriteString("  Default: starts from latest (skips all historical data)\n")

			latestCursor := s.manager.GetCurrentCursor()
			bundledOps := len(index.GetBundles()) * types.BUNDLE_SIZE
			mempoolOps := latestCursor - bundledOps

			if s.config.SyncMode && mempoolOps > 0 {
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
		sb.WriteString(fmt.Sprintf("  curl %s/op/0\n", baseURL))

		if s.config.EnableWebSocket {
			sb.WriteString(fmt.Sprintf("  websocat %s/ws\n", wsURL))
			sb.WriteString(fmt.Sprintf("  websocat '%s/ws?cursor=0'\n", wsURL))
		}

		if s.config.SyncMode {
			sb.WriteString(fmt.Sprintf("  curl %s/status\n", baseURL))
			sb.WriteString(fmt.Sprintf("  curl %s/mempool\n", baseURL))
		}

		sb.WriteString("\n────────────────────────────────────────────────────────────────\n")
		sb.WriteString("https://tangled.org/@atscan.net/plcbundle\n")

		w.Write([]byte(sb.String()))
	}
}

func (s *Server) handleIndexJSON() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		index := s.manager.GetIndex()
		sendJSON(w, 200, index)
	}
}

func (s *Server) handleBundle() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bundleNum, err := strconv.Atoi(r.PathValue("number"))
		if err != nil {
			sendJSON(w, 400, map[string]string{"error": "Invalid bundle number"})
			return
		}

		meta, err := s.manager.GetIndex().GetBundle(bundleNum)
		if err != nil {
			sendJSON(w, 404, map[string]string{"error": "Bundle not found"})
			return
		}

		sendJSON(w, 200, meta)
	}
}

func (s *Server) handleBundleData() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bundleNum, err := strconv.Atoi(r.PathValue("number"))
		if err != nil {
			sendJSON(w, 400, map[string]string{"error": "Invalid bundle number"})
			return
		}

		reader, err := s.manager.StreamBundleRaw(context.Background(), bundleNum)
		if err != nil {
			if strings.Contains(err.Error(), "not in index") || strings.Contains(err.Error(), "not found") {
				sendJSON(w, 404, map[string]string{"error": "Bundle not found"})
			} else {
				sendJSON(w, 500, map[string]string{"error": err.Error()})
			}
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%06d.jsonl.zst", bundleNum))

		io.Copy(w, reader)
	}
}

func (s *Server) handleBundleJSONL() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bundleNum, err := strconv.Atoi(r.PathValue("number"))
		if err != nil {
			sendJSON(w, 400, map[string]string{"error": "Invalid bundle number"})
			return
		}

		reader, err := s.manager.StreamBundleDecompressed(context.Background(), bundleNum)
		if err != nil {
			if strings.Contains(err.Error(), "not in index") || strings.Contains(err.Error(), "not found") {
				sendJSON(w, 404, map[string]string{"error": "Bundle not found"})
			} else {
				sendJSON(w, 500, map[string]string{"error": err.Error()})
			}
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%06d.jsonl", bundleNum))

		io.Copy(w, reader)
	}
}

func (s *Server) handleStatus() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		index := s.manager.GetIndex()
		indexStats := index.GetStats()

		response := StatusResponse{
			Server: ServerStatus{
				Version:          s.config.Version,
				UptimeSeconds:    int(time.Since(s.startTime).Seconds()),
				SyncMode:         s.config.SyncMode,
				WebSocketEnabled: s.config.EnableWebSocket,
				ResolverEnabled:  s.config.EnableResolver,
				Origin:           s.manager.GetPLCOrigin(),
			},
			Bundles: BundleStatus{
				Count:            indexStats["bundle_count"].(int),
				TotalSize:        indexStats["total_size"].(int64),
				UncompressedSize: indexStats["total_uncompressed_size"].(int64),
				UpdatedAt:        indexStats["updated_at"].(time.Time),
			},
		}

		if resolver := s.manager.GetHandleResolver(); resolver != nil {
			response.Server.HandleResolver = resolver.GetBaseURL()
		}

		if s.config.SyncMode && s.config.SyncInterval > 0 {
			response.Server.SyncIntervalSeconds = int(s.config.SyncInterval.Seconds())
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

			totalOps := bundleCount * types.BUNDLE_SIZE
			response.Bundles.TotalOperations = totalOps

			duration := response.Bundles.EndTime.Sub(response.Bundles.StartTime)
			if duration.Hours() > 0 {
				response.Bundles.AvgOpsPerHour = int(float64(totalOps) / duration.Hours())
			}
		}

		if s.config.SyncMode {
			mempoolStats := s.manager.GetMempoolStats()

			if count, ok := mempoolStats["count"].(int); ok {
				mempool := &MempoolStatus{
					Count:            count,
					TargetBundle:     mempoolStats["target_bundle"].(int),
					CanCreateBundle:  mempoolStats["can_create_bundle"].(bool),
					MinTimestamp:     mempoolStats["min_timestamp"].(time.Time),
					Validated:        mempoolStats["validated"].(bool),
					ProgressPercent:  float64(count) / float64(types.BUNDLE_SIZE) * 100,
					BundleSize:       types.BUNDLE_SIZE,
					OperationsNeeded: types.BUNDLE_SIZE - count,
				}

				if firstTime, ok := mempoolStats["first_time"].(time.Time); ok {
					mempool.FirstTime = firstTime
					mempool.TimespanSeconds = int(time.Since(firstTime).Seconds())
				}
				if lastTime, ok := mempoolStats["last_time"].(time.Time); ok {
					mempool.LastTime = lastTime
					mempool.LastOpAgeSeconds = int(time.Since(lastTime).Seconds())
				}

				if count > 100 && count < types.BUNDLE_SIZE {
					if !mempool.FirstTime.IsZero() && !mempool.LastTime.IsZero() {
						timespan := mempool.LastTime.Sub(mempool.FirstTime)
						if timespan.Seconds() > 0 {
							opsPerSec := float64(count) / timespan.Seconds()
							remaining := types.BUNDLE_SIZE - count
							mempool.EtaNextBundleSeconds = int(float64(remaining) / opsPerSec)
						}
					}
				}

				response.Mempool = mempool
			}
		}

		// DID Index stats
		didStats := s.manager.GetDIDIndexStats()
		if didStats["enabled"].(bool) {
			didIndex := &DIDIndexStatus{
				Enabled:      true,
				Exists:       didStats["exists"].(bool),
				TotalDIDs:    didStats["total_dids"].(int64),
				IndexedDIDs:  didStats["indexed_dids"].(int64),
				LastBundle:   didStats["last_bundle"].(int),
				ShardCount:   didStats["shard_count"].(int),
				CachedShards: didStats["cached_shards"].(int),
				CacheLimit:   didStats["cache_limit"].(int),
				UpdatedAt:    didStats["updated_at"].(time.Time),
			}

			// Mempool DIDs
			if mempoolDIDs, ok := didStats["mempool_dids"].(int64); ok && mempoolDIDs > 0 {
				didIndex.MempoolDIDs = mempoolDIDs
			}

			// Version and format
			if s.manager.GetDIDIndex() != nil {
				config := s.manager.GetDIDIndex().GetConfig()
				didIndex.Version = config.Version
				didIndex.Format = config.Format
			}

			// Hot shards
			if cacheOrder, ok := didStats["cache_order"].([]int); ok && len(cacheOrder) > 0 {
				maxShards := 10
				if len(cacheOrder) < maxShards {
					maxShards = len(cacheOrder)
				}
				didIndex.HotShards = cacheOrder[:maxShards]
			}

			// Cache performance
			if cacheHitRate, ok := didStats["cache_hit_rate"].(float64); ok {
				didIndex.CacheHitRate = cacheHitRate
			}
			if cacheHits, ok := didStats["cache_hits"].(int64); ok {
				didIndex.CacheHits = cacheHits
			}
			if cacheMisses, ok := didStats["cache_misses"].(int64); ok {
				didIndex.CacheMisses = cacheMisses
			}
			if totalLookups, ok := didStats["total_lookups"].(int64); ok {
				didIndex.TotalLookups = totalLookups
			}

			// Lookup performance metrics
			if avgTime, ok := didStats["avg_lookup_time_ms"].(float64); ok {
				didIndex.AvgLookupTimeMs = avgTime
			}
			if recentAvg, ok := didStats["recent_avg_lookup_time_ms"].(float64); ok {
				didIndex.RecentAvgLookupTimeMs = recentAvg
			}
			if minTime, ok := didStats["min_lookup_time_ms"].(float64); ok {
				didIndex.MinLookupTimeMs = minTime
			}
			if maxTime, ok := didStats["max_lookup_time_ms"].(float64); ok {
				didIndex.MaxLookupTimeMs = maxTime
			}
			if p50, ok := didStats["p50_lookup_time_ms"].(float64); ok {
				didIndex.P50LookupTimeMs = p50
			}
			if p95, ok := didStats["p95_lookup_time_ms"].(float64); ok {
				didIndex.P95LookupTimeMs = p95
			}
			if p99, ok := didStats["p99_lookup_time_ms"].(float64); ok {
				didIndex.P99LookupTimeMs = p99
			}
			if sampleSize, ok := didStats["recent_sample_size"].(int); ok {
				didIndex.RecentSampleSize = sampleSize
			}

			response.DIDIndex = didIndex
		}

		// Resolver performance stats
		if s.config.EnableResolver {
			resolverStats := s.manager.GetResolverStats()

			if totalRes, ok := resolverStats["total_resolutions"].(int64); ok && totalRes > 0 {
				resolver := &ResolverStatus{
					Enabled:          true,
					TotalResolutions: totalRes,
				}

				// Handle resolver URL
				if hr := s.manager.GetHandleResolver(); hr != nil {
					resolver.HandleResolver = hr.GetBaseURL()
				}

				// Counts
				if v, ok := resolverStats["mempool_hits"].(int64); ok {
					resolver.MempoolHits = v
				}
				if v, ok := resolverStats["bundle_hits"].(int64); ok {
					resolver.BundleHits = v
				}
				if v, ok := resolverStats["errors"].(int64); ok {
					resolver.Errors = v
				}
				if v, ok := resolverStats["success_rate"].(float64); ok {
					resolver.SuccessRate = v
				}
				if v, ok := resolverStats["mempool_hit_rate"].(float64); ok {
					resolver.MempoolHitRate = v
				}

				// Overall averages
				if v, ok := resolverStats["avg_total_time_ms"].(float64); ok {
					resolver.AvgTotalTimeMs = v
				}
				if v, ok := resolverStats["avg_mempool_time_ms"].(float64); ok {
					resolver.AvgMempoolTimeMs = v
				}
				if v, ok := resolverStats["avg_index_time_ms"].(float64); ok {
					resolver.AvgIndexTimeMs = v
				}
				if v, ok := resolverStats["avg_load_op_time_ms"].(float64); ok {
					resolver.AvgLoadOpTimeMs = v
				}

				// Recent averages
				if v, ok := resolverStats["recent_avg_total_time_ms"].(float64); ok {
					resolver.RecentAvgTotalTimeMs = v
				}
				if v, ok := resolverStats["recent_avg_mempool_time_ms"].(float64); ok {
					resolver.RecentAvgMempoolTimeMs = v
				}
				if v, ok := resolverStats["recent_avg_index_time_ms"].(float64); ok {
					resolver.RecentAvgIndexTimeMs = v
				}
				if v, ok := resolverStats["recent_avg_load_time_ms"].(float64); ok {
					resolver.RecentAvgLoadTimeMs = v
				}
				if v, ok := resolverStats["recent_sample_size"].(int); ok {
					resolver.RecentSampleSize = v
				}

				// Percentiles
				if v, ok := resolverStats["min_total_time_ms"].(float64); ok {
					resolver.MinTotalTimeMs = v
				}
				if v, ok := resolverStats["max_total_time_ms"].(float64); ok {
					resolver.MaxTotalTimeMs = v
				}
				if v, ok := resolverStats["p50_total_time_ms"].(float64); ok {
					resolver.P50TotalTimeMs = v
				}
				if v, ok := resolverStats["p95_total_time_ms"].(float64); ok {
					resolver.P95TotalTimeMs = v
				}
				if v, ok := resolverStats["p99_total_time_ms"].(float64); ok {
					resolver.P99TotalTimeMs = v
				}
				if v, ok := resolverStats["p95_index_time_ms"].(float64); ok {
					resolver.P95IndexTimeMs = v
				}
				if v, ok := resolverStats["p95_load_op_time_ms"].(float64); ok {
					resolver.P95LoadOpTimeMs = v
				}

				response.Resolver = resolver
			} else {
				// No resolutions yet, but resolver is enabled
				response.Resolver = &ResolverStatus{
					Enabled:          true,
					TotalResolutions: 0,
				}

				if hr := s.manager.GetHandleResolver(); hr != nil {
					response.Resolver.HandleResolver = hr.GetBaseURL()
				}
			}
		}

		sendJSON(w, 200, response)
	}
}

func (s *Server) handleMempool() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ops, err := s.manager.GetMempoolOperations()
		if err != nil {
			sendJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}

		w.Header().Set("Content-Type", "application/x-ndjson")

		if len(ops) == 0 {
			return
		}

		for _, op := range ops {
			if len(op.RawJSON) > 0 {
				w.Write(op.RawJSON)
			} else {
				data, _ := json.Marshal(op)
				w.Write(data)
			}
			w.Write([]byte("\n"))
		}
	}
}

func (s *Server) handleDebugMemory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var m runtime.MemStats
		runtime.ReadMemStats(&m)

		didStats := s.manager.GetDIDIndexStats()

		beforeAlloc := m.Alloc / 1024 / 1024

		runtime.GC()
		runtime.ReadMemStats(&m)
		afterAlloc := m.Alloc / 1024 / 1024

		response := fmt.Sprintf(`Memory Stats:
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
			afterAlloc)

		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(response))
	}
}

func (s *Server) handleDIDRouting(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/")

	parts := strings.SplitN(path, "/", 2)
	input := parts[0] // Could be DID or handle

	// Accept both DIDs and handles
	// DIDs: did:plc:*, did:web:*
	// Handles: tree.fail, ngerakines.me, etc.

	if len(parts) == 1 {
		s.handleDIDDocument(input)(w, r)
	} else if parts[1] == "data" {
		s.handleDIDData(input)(w, r)
	} else if parts[1] == "log/audit" {
		s.handleDIDAuditLog(input)(w, r)
	} else {
		sendJSON(w, 404, map[string]string{"error": "not found"})
	}
}

func (s *Server) handleDIDDocument(input string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "OPTIONS" {
			return
		}

		// Resolve handle to DID
		did, handleResolveTime, err := s.manager.ResolveHandleOrDID(r.Context(), input)
		if err != nil {
			if strings.Contains(err.Error(), "appears to be a handle") {
				sendJSON(w, 400, map[string]string{
					"error": "Handle resolver not configured",
					"hint":  "Start server with --handle-resolver flag",
				})
			} else {
				sendJSON(w, 400, map[string]string{"error": err.Error()})
			}
			return
		}

		resolvedHandle := ""
		if handleResolveTime > 0 {
			resolvedHandle = input
		}

		// Single call gets both document AND operation metadata
		result, err := s.manager.ResolveDID(r.Context(), did)
		if err != nil {
			if strings.Contains(err.Error(), "deactivated") {
				sendJSON(w, 410, map[string]string{"error": "DID has been deactivated"})
			} else if strings.Contains(err.Error(), "not found") {
				sendJSON(w, 404, map[string]string{"error": "DID not found"})
			} else {
				sendJSON(w, 500, map[string]string{"error": err.Error()})
			}
			return
		}

		// Early ETag check - operation is already in result.LatestOperation
		if result.LatestOperation != nil {
			etag := fmt.Sprintf(`"%s"`, result.LatestOperation.CID)

			if match := r.Header.Get("If-None-Match"); match != "" {
				// Strip quotes if present
				matchClean := strings.Trim(match, `"`)
				if matchClean == result.LatestOperation.CID {
					// Set minimal headers for 304 response
					w.Header().Set("ETag", etag)
					w.Header().Set("Cache-Control", "public, max-age=300")
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}

		// Set all headers (now with result.LatestOperation available)
		setDIDDocumentHeaders(w, r, did, resolvedHandle, result, handleResolveTime)

		w.Header().Set("Content-Type", "application/did+ld+json")
		sendJSON(w, 200, result.Document)
	}
}

func (s *Server) handleDIDData(input string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Resolve handle to DID
		did, _, err := s.manager.ResolveHandleOrDID(r.Context(), input)
		if err != nil {
			sendJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}

		operations, err := s.manager.GetDIDOperations(context.Background(), did, false)
		if err != nil {
			sendJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}

		if len(operations) == 0 {
			sendJSON(w, 404, map[string]string{"error": "DID not found"})
			return
		}

		state, err := plcclient.BuildDIDState(did, operations)
		if err != nil {
			if strings.Contains(err.Error(), "deactivated") {
				sendJSON(w, 410, map[string]string{"error": "DID has been deactivated"})
			} else {
				sendJSON(w, 500, map[string]string{"error": err.Error()})
			}
			return
		}

		sendJSON(w, 200, state)
	}
}

func (s *Server) handleDIDAuditLog(input string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Resolve handle to DID
		did, _, err := s.manager.ResolveHandleOrDID(r.Context(), input)
		if err != nil {
			sendJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}

		operations, err := s.manager.GetDIDOperations(context.Background(), did, false)
		if err != nil {
			sendJSON(w, 500, map[string]string{"error": err.Error()})
			return
		}

		if len(operations) == 0 {
			sendJSON(w, 404, map[string]string{"error": "DID not found"})
			return
		}

		auditLog := plcclient.FormatAuditLog(operations)
		sendJSON(w, 200, auditLog)
	}
}

// handleOperation gets a single operation with detailed timing headers
func (s *Server) handleOperation() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		pointer := r.PathValue("pointer")

		// Parse pointer format: "bundle:position" or global position
		bundleNum, position, err := parseOperationPointer(pointer)
		if err != nil {
			sendJSON(w, 400, map[string]string{"error": err.Error()})
			return
		}

		// Validate position range
		if position < 0 || position >= types.BUNDLE_SIZE {
			sendJSON(w, 400, map[string]string{
				"error": fmt.Sprintf("Position must be 0-%d", types.BUNDLE_SIZE-1),
			})
			return
		}

		// Time the entire request
		totalStart := time.Now()

		// Time the operation load
		loadStart := time.Now()
		op, err := s.manager.LoadOperation(r.Context(), bundleNum, position)
		loadDuration := time.Since(loadStart)

		if err != nil {
			if strings.Contains(err.Error(), "not in index") ||
				strings.Contains(err.Error(), "not found") {
				sendJSON(w, 404, map[string]string{"error": "Operation not found"})
			} else {
				sendJSON(w, 500, map[string]string{"error": err.Error()})
			}
			return
		}

		totalDuration := time.Since(totalStart)

		// Calculate global position
		globalPos := (bundleNum * types.BUNDLE_SIZE) + position

		// Calculate operation age
		opAge := time.Since(op.CreatedAt)

		// Set response headers with useful metadata
		setOperationHeaders(w, op, bundleNum, position, globalPos, loadDuration, totalDuration, opAge)

		// Send raw JSON if available (faster, preserves exact format)
		if len(op.RawJSON) > 0 {
			w.Header().Set("Content-Type", "application/json")
			w.Write(op.RawJSON)
		} else {
			sendJSON(w, 200, op)
		}
	}
}

// parseOperationPointer parses pointer in format "bundle:position" or global position
func parseOperationPointer(pointer string) (bundleNum, position int, err error) {
	// Check if it's the "bundle:position" format
	if strings.Contains(pointer, ":") {
		parts := strings.Split(pointer, ":")
		if len(parts) != 2 {
			return 0, 0, fmt.Errorf("invalid pointer format: use 'bundle:position' or global position")
		}

		bundleNum, err = strconv.Atoi(parts[0])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid bundle number: %w", err)
		}

		position, err = strconv.Atoi(parts[1])
		if err != nil {
			return 0, 0, fmt.Errorf("invalid position: %w", err)
		}

		if bundleNum < 1 {
			return 0, 0, fmt.Errorf("bundle number must be >= 1")
		}

		return bundleNum, position, nil
	}

	// Parse as global position
	globalPos, err := strconv.Atoi(pointer)
	if err != nil {
		return 0, 0, fmt.Errorf("invalid position: must be number or 'bundle:position' format")
	}

	if globalPos < 0 {
		return 0, 0, fmt.Errorf("global position must be >= 0")
	}

	// Handle small numbers as shorthand for bundle 1
	if globalPos < types.BUNDLE_SIZE {
		return 1, globalPos, nil
	}

	// Convert global position to bundle + position
	bundleNum = globalPos / types.BUNDLE_SIZE
	position = globalPos % types.BUNDLE_SIZE

	// Minimum bundle number is 1
	if bundleNum < 1 {
		bundleNum = 1
	}

	return bundleNum, position, nil
}

// setOperationHeaders sets useful response headers
func setOperationHeaders(
	w http.ResponseWriter,
	op *plcclient.PLCOperation,
	bundleNum, position, globalPos int,
	loadDuration, totalDuration, opAge time.Duration,
) {
	// === Location Information ===
	w.Header().Set("X-Bundle-Number", fmt.Sprintf("%d", bundleNum))
	w.Header().Set("X-Position", fmt.Sprintf("%d", position))
	w.Header().Set("X-Global-Position", fmt.Sprintf("%d", globalPos))
	w.Header().Set("X-Pointer", fmt.Sprintf("%d:%d", bundleNum, position))

	// === Operation Metadata ===
	w.Header().Set("X-Operation-DID", op.DID)
	w.Header().Set("X-Operation-CID", op.CID)
	w.Header().Set("X-Operation-Created", op.CreatedAt.Format(time.RFC3339))
	w.Header().Set("X-Operation-Age-Seconds", fmt.Sprintf("%d", int(opAge.Seconds())))

	// Nullification status
	if op.IsNullified() {
		w.Header().Set("X-Operation-Nullified", "true")
		if nullCID := op.GetNullifyingCID(); nullCID != "" {
			w.Header().Set("X-Operation-Nullified-By", nullCID)
		}
	} else {
		w.Header().Set("X-Operation-Nullified", "false")
	}

	// === Size Information ===
	if len(op.RawJSON) > 0 {
		w.Header().Set("X-Operation-Size", fmt.Sprintf("%d", len(op.RawJSON)))
	}

	// === Performance Metrics (in milliseconds with precision) ===
	w.Header().Set("X-Load-Time-Ms", fmt.Sprintf("%.3f", float64(loadDuration.Microseconds())/1000.0))
	w.Header().Set("X-Total-Time-Ms", fmt.Sprintf("%.3f", float64(totalDuration.Microseconds())/1000.0))

	// === Caching Hints ===
	// Set cache control (operations are immutable once bundled)
	w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
	w.Header().Set("ETag", op.CID) // CID is perfect for ETag
}

// handleDIDIndexStats returns detailed DID index performance metrics
func (s *Server) handleDebugDIDIndex() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		didStats := s.manager.GetDIDIndexStats()

		if !didStats["enabled"].(bool) || !didStats["exists"].(bool) {
			sendJSON(w, 404, map[string]string{
				"error": "DID index not available",
			})
			return
		}

		// Return all stats (more detailed than /status)
		sendJSON(w, 200, didStats)
	}
}

func (s *Server) handleDebugResolver() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		resolverStats := s.manager.GetResolverStats()

		if resolverStats == nil {
			sendJSON(w, 404, map[string]string{
				"error": "Resolver not enabled",
			})
			return
		}

		// Return all stats (more detailed than /status)
		sendJSON(w, 200, resolverStats)
	}
}
