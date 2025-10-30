// detector/script.go
package detector

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"tangled.org/atscan.net/plcbundle/plc"
)

// ScriptDetector runs a JavaScript detector via Unix socket
type ScriptDetector struct {
	name       string
	scriptPath string
	socketPath string
	serverCmd  *exec.Cmd
	conn       net.Conn
	writer     *bufio.Writer
	reader     *bufio.Reader
}

func NewScriptDetector(scriptPath string) (*ScriptDetector, error) {
	// Verify bun is available
	if _, err := exec.LookPath("bun"); err != nil {
		return nil, fmt.Errorf("bun runtime not found in PATH")
	}

	absPath, err := filepath.Abs(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("invalid script path: %w", err)
	}

	// Use filename as detector name
	name := strings.TrimSuffix(filepath.Base(scriptPath), filepath.Ext(scriptPath))

	// Create unique socket path
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("detector-%d-%s.sock", os.Getpid(), name))

	sd := &ScriptDetector{
		name:       name,
		scriptPath: absPath,
		socketPath: socketPath,
	}

	// Start the server
	if err := sd.startServer(); err != nil {
		return nil, err
	}

	return sd, nil
}

func (d *ScriptDetector) Name() string        { return d.name }
func (d *ScriptDetector) Description() string { return "JavaScript detector: " + d.name }
func (d *ScriptDetector) Version() string     { return "1.0.0" }

func (d *ScriptDetector) startServer() error {
	// Read user's script
	userCode, err := os.ReadFile(d.scriptPath)
	if err != nil {
		return fmt.Errorf("failed to read script: %w", err)
	}

	// Create wrapper script with socket server
	wrapperScript := d.createSocketWrapper(string(userCode))

	// Remove old socket if exists
	os.Remove(d.socketPath)

	// Start Bun server with script piped to stdin
	d.serverCmd = exec.Command("bun", "run", "-", d.socketPath)
	d.serverCmd.Stdout = os.Stderr
	d.serverCmd.Stderr = os.Stderr

	// Get stdin pipe
	stdin, err := d.serverCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	// Start the process
	if err := d.serverCmd.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	// Write wrapper script to stdin
	if _, err := stdin.Write([]byte(wrapperScript)); err != nil {
		d.serverCmd.Process.Kill()
		return fmt.Errorf("failed to write script: %w", err)
	}
	stdin.Close()

	// Wait for socket to be ready and connect
	if err := d.connectToServer(); err != nil {
		d.serverCmd.Process.Kill()
		os.Remove(d.socketPath)
		return err
	}

	return nil
}

func (d *ScriptDetector) createSocketWrapper(userCode string) string {
	return fmt.Sprintf(`// Auto-generated socket server wrapper
// User's detect function
%s

// Unix socket server
const socketPath = process.argv[2];

// Clean up old socket
try {
  await Bun.file(socketPath).unlink();
} catch {}

const server = Bun.listen({
  unix: socketPath,
  socket: {
    data(socket, data) {
      const lines = data.toString().split('\n').filter(line => line.trim());
      
      for (const line of lines) {
        try {
          const operation = JSON.parse(line);
          const labels = detect({ op: operation }) || [];
          socket.write(JSON.stringify({ labels }) + '\n');
        } catch (error) {
          socket.write(JSON.stringify({ labels: [], error: error.message }) + '\n');
        }
      }
    },
    error(socket, error) {
      console.error('Socket error:', error);
    },
    close(socket) {
      console.error('Socket closed');
    }
  }
});

console.error('Detector server ready on socket:', socketPath);
`, userCode)
}

func (d *ScriptDetector) connectToServer() error {
	// Try to connect with retries
	maxRetries := 50
	for i := 0; i < maxRetries; i++ {
		conn, err := net.Dial("unix", d.socketPath)
		if err == nil {
			d.conn = conn
			d.writer = bufio.NewWriter(conn)
			d.reader = bufio.NewReader(conn)
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("failed to connect to socket within timeout")
}

func (d *ScriptDetector) Detect(ctx context.Context, op plc.PLCOperation) (*Match, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("not connected to server")
	}

	// Serialize operation
	data, err := json.Marshal(op)
	if err != nil {
		return nil, fmt.Errorf("failed to serialize operation: %w", err)
	}

	// Write to socket with newline delimiter
	if _, err := d.writer.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write to socket: %w", err)
	}
	if _, err := d.writer.WriteString("\n"); err != nil {
		return nil, fmt.Errorf("failed to write newline: %w", err)
	}
	if err := d.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	// Read response
	line, err := d.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

	// Parse response
	var result struct {
		Labels []string `json:"labels"`
		Error  string   `json:"error,omitempty"`
	}

	if err := json.Unmarshal([]byte(line), &result); err != nil {
		return nil, fmt.Errorf("failed to parse response: %w", err)
	}

	if result.Error != "" {
		return nil, fmt.Errorf("detector error: %s", result.Error)
	}

	// No match if no labels
	if len(result.Labels) == 0 {
		return nil, nil
	}

	// Convert labels to Match
	return &Match{
		Reason:     strings.Join(result.Labels, "_"),
		Category:   "custom",
		Confidence: 0.95,
		Note:       fmt.Sprintf("Labels: %s", strings.Join(result.Labels, ", ")),
		Metadata: map[string]interface{}{
			"labels":   result.Labels,
			"detector": d.name,
		},
	}, nil
}

// Close shuts down the server
func (d *ScriptDetector) Close() error {
	// Close connection
	if d.conn != nil {
		d.conn.Close()
		d.conn = nil
	}

	// Kill server process
	if d.serverCmd != nil && d.serverCmd.Process != nil {
		d.serverCmd.Process.Kill()
		d.serverCmd.Wait()
	}

	// Remove socket file
	os.Remove(d.socketPath)

	return nil
}
