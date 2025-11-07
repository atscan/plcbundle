package server

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/goccy/go-json"
	"github.com/gorilla/websocket"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
	"tangled.org/atscan.net/plcbundle/internal/types"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func (s *Server) handleWebSocket() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cursorStr := r.URL.Query().Get("cursor")
		var cursor int

		if cursorStr == "" {
			cursor = s.manager.GetCurrentCursor()
		} else {
			var err error
			cursor, err = strconv.Atoi(cursorStr)
			if err != nil || cursor < 0 {
				http.Error(w, "Invalid cursor: must be non-negative integer", 400)
				return
			}
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

		if err := s.streamLive(bgCtx, conn, cursor, done); err != nil {
			fmt.Fprintf(os.Stderr, "WebSocket stream error: %v\n", err)
		}
	}
}

func (s *Server) streamLive(ctx context.Context, conn *websocket.Conn, startCursor int, done chan struct{}) error {
	index := s.manager.GetIndex()
	bundles := index.GetBundles()
	currentRecord := startCursor

	// Stream existing bundles
	if len(bundles) > 0 {
		startBundleIdx := startCursor / types.BUNDLE_SIZE
		startPosition := startCursor % types.BUNDLE_SIZE

		if startBundleIdx < len(bundles) {
			for i := startBundleIdx; i < len(bundles); i++ {
				skipUntil := 0
				if i == startBundleIdx {
					skipUntil = startPosition
				}

				newRecordCount, err := s.streamBundle(ctx, conn, bundles[i].BundleNumber, skipUntil, done)
				if err != nil {
					return err
				}
				currentRecord += newRecordCount
			}
		}
	}

	lastSeenMempoolCount := 0
	if err := s.streamMempool(conn, startCursor, len(bundles)*types.BUNDLE_SIZE, &currentRecord, &lastSeenMempoolCount, done); err != nil {
		return err
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	lastBundleCount := len(bundles)

	for {
		select {
		case <-done:
			return nil

		case <-ticker.C:
			index = s.manager.GetIndex()
			bundles = index.GetBundles()

			if len(bundles) > lastBundleCount {
				newBundleCount := len(bundles) - lastBundleCount
				currentRecord += newBundleCount * types.BUNDLE_SIZE
				lastBundleCount = len(bundles)
				lastSeenMempoolCount = 0
			}

			if err := s.streamMempool(conn, startCursor, len(bundles)*types.BUNDLE_SIZE, &currentRecord, &lastSeenMempoolCount, done); err != nil {
				return err
			}

			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return err
			}
		}
	}
}

func (s *Server) streamBundle(ctx context.Context, conn *websocket.Conn, bundleNumber int, skipUntil int, done chan struct{}) (int, error) {
	reader, err := s.manager.StreamBundleDecompressed(ctx, bundleNumber)
	if err != nil {
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

func (s *Server) streamMempool(conn *websocket.Conn, startCursor int, bundleRecordBase int, currentRecord *int, lastSeenCount *int, done chan struct{}) error {
	mempoolOps, err := s.manager.GetMempoolOperations()
	if err != nil {
		return nil
	}

	if len(mempoolOps) <= *lastSeenCount {
		return nil
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

func sendOperation(conn *websocket.Conn, op plcclient.PLCOperation) error {
	var data []byte
	var err error

	if len(op.RawJSON) > 0 {
		data = op.RawJSON
	} else {
		data, err = json.Marshal(op)
		if err != nil {
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
