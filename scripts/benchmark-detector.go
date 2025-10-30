package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	gozstd "github.com/DataDog/zstd"
	"github.com/goccy/go-json"
)

// Minimal operation struct
type Operation struct {
	DID string `json:"did"`
	//Operation map[string]interface{} `json:"operation"`
	CID       string      `json:"cid"`
	Nullified interface{} `json:"nullified,omitempty"`
	CreatedAt time.Time   `json:"createdAt"`

	// RawJSON stores the original JSON bytes for exact reproduction
	RawJSON []byte `json:"-"`
}

// User's detect function
func detect(op *Operation) []string {
	labels := []string{}

	if strings.HasPrefix(op.DID, "did:plc:aa") {
		labels = append(labels, "test")
	}

	return labels
}

func main() {
	bundleDir := "./"
	if len(os.Args) > 1 {
		bundleDir = os.Args[1]
	}

	startBundle := 1
	if len(os.Args) > 2 {
		startBundle, _ = strconv.Atoi(os.Args[2])
	}

	endBundle := 100
	if len(os.Args) > 3 {
		endBundle, _ = strconv.Atoi(os.Args[3])
	}

	fmt.Fprintf(os.Stderr, "Processing bundles %d-%d from %s\n", startBundle, endBundle, bundleDir)
	fmt.Fprintf(os.Stderr, "\n")

	// Buffered stdout
	writer := bufio.NewWriterSize(os.Stdout, 512*1024)
	defer writer.Flush()

	// CSV header
	writer.WriteString("bundle,position,cid,size,confidence,labels\n")

	totalOps := 0
	matchCount := 0
	totalBytes := int64(0)
	matchedBytes := int64(0)

	startTime := time.Now()

	for bundleNum := startBundle; bundleNum <= endBundle; bundleNum++ {
		bundleFile := filepath.Join(bundleDir, fmt.Sprintf("%06d.jsonl.zst", bundleNum))

		// Read compressed file
		compressed, err := os.ReadFile(bundleFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError reading bundle %d: %v\n", bundleNum, err)
			continue
		}

		// Decompress with native gozstd (CGO, fast as native zstd)
		decompressed, err := gozstd.Decompress(nil, compressed)
		if err != nil {
			fmt.Fprintf(os.Stderr, "\nError decompressing bundle %d: %v\n", bundleNum, err)
			continue
		}

		// Split into lines
		lines := bytes.Split(decompressed, []byte("\n"))

		for position, line := range lines {
			if len(line) == 0 {
				continue
			}

			totalOps++
			opSize := len(line)
			totalBytes += int64(opSize)

			// Parse JSON with go-json
			var op Operation
			if err := json.Unmarshal(line, &op); err != nil {
				fmt.Fprintf(os.Stderr, "Error parsing operation: %v\n", err)
				continue
			}

			// Run detect function
			labels := detect(&op)

			if len(labels) > 0 {
				matchCount++
				matchedBytes += int64(opSize)

				// Extract last 4 chars of CID
				cidShort := op.CID
				if len(cidShort) > 4 {
					cidShort = cidShort[len(cidShort)-4:]
				}

				// Write CSV line
				writer.WriteString(strconv.Itoa(bundleNum))
				writer.WriteByte(',')
				writer.WriteString(strconv.Itoa(position))
				writer.WriteByte(',')
				writer.WriteString(cidShort)
				writer.WriteByte(',')
				writer.WriteString(strconv.Itoa(opSize))
				writer.WriteByte(',')
				writer.WriteString("0.95")
				writer.WriteByte(',')
				writer.WriteString(strings.Join(labels, ";"))
				writer.WriteByte('\n')
			}
		}

		// Progress
		if bundleNum%10 == 0 {
			elapsed := time.Since(startTime).Seconds()
			opsPerSec := float64(totalOps) / elapsed
			fmt.Fprintf(os.Stderr, "Processed %d/%d bundles | %d ops | %.0f ops/sec\r",
				bundleNum, endBundle, totalOps, opsPerSec)
		}
	}

	elapsed := time.Since(startTime).Seconds()

	// Stats
	fmt.Fprintf(os.Stderr, "\n\n")
	fmt.Fprintf(os.Stderr, "✓ Detection complete\n")
	fmt.Fprintf(os.Stderr, "  Total operations:   %d\n", totalOps)
	fmt.Fprintf(os.Stderr, "  Matches found:      %d (%.2f%%)\n", matchCount, float64(matchCount)/float64(totalOps)*100)
	fmt.Fprintf(os.Stderr, "  Total size:         %.1f MB\n", float64(totalBytes)/1e6)
	fmt.Fprintf(os.Stderr, "  Matched size:       %.1f MB (%.2f%%)\n", float64(matchedBytes)/1e6, float64(matchedBytes)/float64(totalBytes)*100)
	fmt.Fprintf(os.Stderr, "\n")
	fmt.Fprintf(os.Stderr, "  Time elapsed:       %.2fs\n", elapsed)
	fmt.Fprintf(os.Stderr, "  Throughput:         %.0f ops/sec\n", float64(totalOps)/elapsed)
	fmt.Fprintf(os.Stderr, "  Speed:              %.1f MB/sec\n", float64(totalBytes)/elapsed/1e6)
}
