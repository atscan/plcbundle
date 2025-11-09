// detector/script.go
package detector

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"
	"tangled.org/atscan.net/plcbundle/internal/plcclient"
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
	mu         sync.Mutex
}

func NewScriptDetector(scriptPath string) (*ScriptDetector, error) {
	if _, err := exec.LookPath("bun"); err != nil {
		return nil, fmt.Errorf("bun runtime not found in PATH")
	}

	absPath, err := filepath.Abs(scriptPath)
	if err != nil {
		return nil, fmt.Errorf("invalid script path: %w", err)
	}

	name := strings.TrimSuffix(filepath.Base(scriptPath), filepath.Ext(scriptPath))
	socketPath := filepath.Join(os.TempDir(), fmt.Sprintf("detector-%d-%s.sock", os.Getpid(), name))

	sd := &ScriptDetector{
		name:       name,
		scriptPath: absPath,
		socketPath: socketPath,
	}

	if err := sd.startServer(); err != nil {
		return nil, err
	}

	return sd, nil
}

func (d *ScriptDetector) Name() string        { return d.name }
func (d *ScriptDetector) Description() string { return "JavaScript detector: " + d.name }
func (d *ScriptDetector) Version() string     { return "1.0.0" }

func (d *ScriptDetector) startServer() error {
	userCode, err := os.ReadFile(d.scriptPath)
	if err != nil {
		return fmt.Errorf("failed to read script: %w", err)
	}

	wrapperScript := d.createSocketWrapper(string(userCode))
	os.Remove(d.socketPath)

	d.serverCmd = exec.Command("bun", "run", "-", d.socketPath)
	d.serverCmd.Stdout = os.Stderr
	d.serverCmd.Stderr = os.Stderr

	stdin, err := d.serverCmd.StdinPipe()
	if err != nil {
		return fmt.Errorf("failed to create stdin pipe: %w", err)
	}

	if err := d.serverCmd.Start(); err != nil {
		return fmt.Errorf("failed to start server: %w", err)
	}

	stdin.Write([]byte(wrapperScript))
	stdin.Close()

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

try {
  await Bun.file(socketPath).unlink();
} catch {}

const server = Bun.listen({
  unix: socketPath,
  socket: {
    data(socket, data) {
      try {
        const operation = JSON.parse(data.toString());
        const labels = detect({ op: operation }) || [];
        socket.write(JSON.stringify({ labels }) + '\n');
      } catch (error) {
        socket.write(JSON.stringify({ labels: [], error: error.message }) + '\n');
      }
    },
    error(socket, error) {},
    close(socket) {}
  }
});

console.error('Detector server ready on socket:', socketPath);
`, userCode)
}

func (d *ScriptDetector) connectToServer() error {
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

func (d *ScriptDetector) Detect(ctx context.Context, op plcclient.PLCOperation) (*Match, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("not connected to server")
	}

	// ✨ LOCK for entire socket communication
	d.mu.Lock()
	defer d.mu.Unlock()

	// Use RawJSON directly
	data := op.RawJSON
	if len(data) == 0 {
		var err error
		data, err = json.Marshal(op)
		if err != nil {
			return nil, fmt.Errorf("failed to serialize operation: %w", err)
		}
	}

	if _, err := d.writer.Write(data); err != nil {
		return nil, fmt.Errorf("failed to write to socket: %w", err)
	}
	if _, err := d.writer.WriteString("\n"); err != nil {
		return nil, fmt.Errorf("failed to write newline: %w", err)
	}
	if err := d.writer.Flush(); err != nil {
		return nil, fmt.Errorf("failed to flush: %w", err)
	}

	line, err := d.reader.ReadString('\n')
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %w", err)
	}

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

	if len(result.Labels) == 0 {
		return nil, nil
	}

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

func (d *ScriptDetector) Close() error {
	if d.conn != nil {
		d.conn.Close()
		d.conn = nil
	}

	if d.serverCmd != nil && d.serverCmd.Process != nil {
		d.serverCmd.Process.Kill()
		d.serverCmd.Wait()
	}

	os.Remove(d.socketPath)
	return nil
}
