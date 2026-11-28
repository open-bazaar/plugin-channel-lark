package lark_test

import (
	"archive/zip"
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

var excludedTopLevel = map[string]bool{
	"README.md":         true,
	"CHANGELOG.md":      true,
	"screenshots":       true,
	"docs":              true,
	"dist":              true,
	"scripts":           true,
	"tests":             true,
	"test":              true,
	"node_modules":      true,
	"package.json":      true,
	"package-lock.json": true,
	"tsconfig.json":     true,
	"plugin.zip":        true,
	"plugin.zip.sha256": true,
	"SHA256SUMS.txt":    true,
}

var (
	readmeRe       = regexp.MustCompile(`(?i)^README(?:\.[^.]+)?\.md$`)
	ansiRe         = regexp.MustCompile(`\x1b\[[0-9;]*[a-zA-Z]`)
	approvalHashRe = regexp.MustCompile(`Approval Hash:?\s+([0-9a-f]{64})`)
)

func isExcludedRel(relPosix string) bool {
	parts := strings.Split(relPosix, "/")
	if len(parts) == 0 || relPosix == "" {
		return true
	}
	for _, p := range parts {
		if strings.HasPrefix(p, ".") {
			return true
		}
	}
	if excludedTopLevel[parts[0]] {
		return true
	}
	if len(parts) == 1 && readmeRe.MatchString(parts[0]) {
		return true
	}
	name := parts[len(parts)-1]
	for _, suffix := range []string{
		".test.lua", ".spec.lua", "_test.lua", "_spec.lua",
		".test.js", ".test.ts", ".spec.js",
	} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}

func packagePluginDir(t *testing.T, rootDir, outZip string) []string {
	t.Helper()
	var rels []string
	err := filepath.WalkDir(rootDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == rootDir {
			return nil
		}
		rel, err := filepath.Rel(rootDir, path)
		if err != nil {
			return err
		}
		relPosix := filepath.ToSlash(rel)
		if isExcludedRel(relPosix) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		rels = append(rels, relPosix)
		return nil
	})
	if err != nil {
		t.Fatalf("walk plugin dir %s: %v", rootDir, err)
	}
	sort.Strings(rels)

	f, err := os.Create(outZip)
	if err != nil {
		t.Fatalf("create zip %s: %v", outZip, err)
	}
	zw := zip.NewWriter(f)
	for _, rel := range rels {
		data, err := os.ReadFile(filepath.Join(rootDir, filepath.FromSlash(rel)))
		if err != nil {
			_ = zw.Close()
			_ = f.Close()
			t.Fatalf("read %s: %v", rel, err)
		}
		hdr := &zip.FileHeader{
			Name:     rel,
			Method:   zip.Deflate,
			Modified: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
		}
		hdr.SetMode(0o644)
		w, err := zw.CreateHeader(hdr)
		if err != nil {
			_ = zw.Close()
			_ = f.Close()
			t.Fatalf("zip create %s: %v", rel, err)
		}
		if _, err := w.Write(data); err != nil {
			_ = zw.Close()
			_ = f.Close()
			t.Fatalf("zip write %s: %v", rel, err)
		}
	}
	if err := zw.Close(); err != nil {
		_ = f.Close()
		t.Fatalf("close zip writer: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close zip file: %v", err)
	}
	return rels
}

func resolveBCNBin(t *testing.T) string {
	t.Helper()
	if envBin := strings.TrimSpace(os.Getenv("BCN_BIN")); envBin != "" {
		if _, err := os.Stat(envBin); err != nil {
			t.Fatalf("BCN_BIN=%q not accessible: %v", envBin, err)
		}
		return envBin
	}
	p, err := exec.LookPath("bcn")
	if err != nil {
		t.Fatalf("bcn binary not found in PATH and BCN_BIN is unset: %v", err)
	}
	return p
}

func cleanEnv(extra map[string]string) []string {
	var out []string
	for _, kv := range os.Environ() {
		k, _, ok := strings.Cut(kv, "=")
		if !ok {
			continue
		}
		ku := strings.ToUpper(k)
		if strings.HasPrefix(ku, "BCN_") ||
			strings.HasPrefix(ku, "BCC_") ||
			strings.HasPrefix(ku, "LARK_") ||
			strings.HasPrefix(ku, "CLAUDE_") ||
			strings.HasPrefix(ku, "ANTHROPIC_") ||
			strings.HasPrefix(ku, "OPENAI_") {
			continue
		}
		out = append(out, kv)
	}
	out = append(out, "NO_COLOR=1")
	for k, v := range extra {
		out = append(out, k+"="+v)
	}
	return out
}

func runBCN(t *testing.T, bcnBin, homeDir string, extraEnv map[string]string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	fullArgs := append([]string{"--home", homeDir}, args...)
	cmd := exec.CommandContext(ctx, bcnBin, fullArgs...)
	cmd.Env = cleanEnv(extraEnv)
	out, err := cmd.CombinedOutput()
	plain := ansiRe.ReplaceAllString(string(out), "")
	if err != nil {
		t.Fatalf("bcn %v failed: %v\noutput:\n%s", args, err, plain)
	}
	return plain
}

func installPluginZip(t *testing.T, bcnBin, homeDir, zipPath string) {
	t.Helper()
	insp := runBCN(t, bcnBin, homeDir, nil, "plugin", "inspect", "--dry-run", zipPath)
	m := approvalHashRe.FindStringSubmatch(insp)
	if len(m) != 2 {
		t.Fatalf("could not parse Approval Hash from bcn plugin inspect:\n%s", insp)
	}
	runBCN(t, bcnBin, homeDir, nil, "plugin", "install", zipPath, "--approve-hash", m[1])
}

type receiptOverrides struct {
	ExecOverrides map[string]string `json:"execOverrides,omitempty"`
	Network       []string          `json:"network,omitempty"`
	PrivateHosts  []string          `json:"privateHosts,omitempty"`
}

func patchReceiptOverrides(t *testing.T, homeDir, pluginID string, ov receiptOverrides) {
	t.Helper()
	receiptsPath := filepath.Join(homeDir, "plugins", "receipts.json")
	raw, err := os.ReadFile(receiptsPath)
	if err != nil {
		t.Fatalf("read receipts.json: %v", err)
	}
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse receipts.json: %v", err)
	}
	receipts := doc["receipts"].(map[string]any)
	rec := receipts[pluginID].(map[string]any)
	ovBytes, _ := json.Marshal(ov)
	var ovMap map[string]any
	_ = json.Unmarshal(ovBytes, &ovMap)
	rec["overrides"] = ovMap
	updated, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		t.Fatalf("marshal receipts.json: %v", err)
	}
	if err := os.WriteFile(receiptsPath, append(updated, '\n'), 0o600); err != nil {
		t.Fatalf("write receipts.json: %v", err)
	}
}

type auditEvent struct {
	Seq       int64          `json:"seq"`
	RunID     string         `json:"runId"`
	Type      string         `json:"type"`
	AgentID   string         `json:"agentId"`
	ActorID   string         `json:"actorId"`
	PluginID  string         `json:"pluginId"`
	Payload   map[string]any `json:"payload"`
	CreatedAt int64          `json:"createdAt"`
}

type reviewConversation struct {
	ID              string `json:"id"`
	AgentID         string `json:"agentId"`
	ChannelID       string `json:"channelId"`
	Target          string `json:"target"`
	ConversationKey string `json:"conversationKey"`
	ProviderTarget  string `json:"providerTarget"`
	Kind            string `json:"kind"`
	DisplayName     string `json:"displayName"`
	ReviewStatus    string `json:"reviewStatus"`
}

type runningBCN struct {
	t       *testing.T
	homeDir string
	logPath string
	cmd     *exec.Cmd
	logFile *os.File
	done    chan error
	stopMu  sync.Mutex
	stopped bool
}

func artifactDir(t *testing.T, subdir string) string {
	t.Helper()
	base := strings.TrimSpace(os.Getenv("BCN_E2E_ARTIFACTS"))
	if base == "" {
		return t.TempDir()
	}
	dir := filepath.Join(base, subdir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir artifact dir %s: %v", dir, err)
	}
	return dir
}

func startBCNNode(t *testing.T, bcnBin, homeDir string, extraEnv map[string]string, logPath string) *runningBCN {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(logPath), 0o755); err != nil {
		t.Fatalf("mkdir log dir: %v", err)
	}
	lf, err := os.Create(logPath)
	if err != nil {
		t.Fatalf("create node log: %v", err)
	}
	cmd := exec.Command(bcnBin, "--home", homeDir, "run")
	cmd.Env = cleanEnv(extraEnv)
	cmd.Stdout = lf
	cmd.Stderr = lf
	if err := cmd.Start(); err != nil {
		_ = lf.Close()
		t.Fatalf("start bcn run: %v", err)
	}
	node := &runningBCN{
		t:       t,
		homeDir: homeDir,
		logPath: logPath,
		cmd:     cmd,
		logFile: lf,
		done:    make(chan error, 1),
	}
	go func() {
		node.done <- cmd.Wait()
	}()

	deadline := time.Now().Add(45 * time.Second)
	adminFile := filepath.Join(homeDir, "runtime", "admin-ipc.json")
	for time.Now().Before(deadline) {
		select {
		case err := <-node.done:
			node.done <- err
			raw, _ := os.ReadFile(logPath)
			t.Fatalf("bcn run exited before ready (%v):\n%s", err, string(raw))
		default:
		}
		raw, _ := os.ReadFile(logPath)
		if bytes.Contains(raw, []byte("bcn ready")) {
			if _, statErr := os.Stat(adminFile); statErr == nil {
				return node
			}
		}
		time.Sleep(150 * time.Millisecond)
	}
	_ = node.Stop()
	raw, _ := os.ReadFile(logPath)
	t.Fatalf("timed out waiting for bcn ready:\n%s", string(raw))
	return nil
}

func (n *runningBCN) PID() int {
	if n.cmd != nil && n.cmd.Process != nil {
		return n.cmd.Process.Pid
	}
	return 0
}

func (n *runningBCN) Stop() error {
	n.stopMu.Lock()
	if n.stopped {
		n.stopMu.Unlock()
		return nil
	}
	n.stopped = true
	n.stopMu.Unlock()
	if n.cmd != nil && n.cmd.Process != nil {
		if runtime.GOOS == "windows" {
			_ = n.cmd.Process.Kill()
		} else {
			_ = n.cmd.Process.Signal(syscall.SIGTERM)
		}
	}
	var waitErr error
	select {
	case waitErr = <-n.done:
	case <-time.After(15 * time.Second):
		if n.cmd != nil && n.cmd.Process != nil {
			_ = n.cmd.Process.Kill()
		}
		waitErr = <-n.done
	}
	if n.logFile != nil {
		_ = n.logFile.Close()
	}
	return waitErr
}

func (n *runningBCN) LogText() string {
	raw, _ := os.ReadFile(n.logPath)
	return string(raw)
}

func (n *runningBCN) adminCall(method string, params any, resultOut any) error {
	raw, err := os.ReadFile(filepath.Join(n.homeDir, "runtime", "admin-ipc.json"))
	if err != nil {
		return err
	}
	var ep struct {
		Endpoint string `json:"endpoint"`
		Token    string `json:"token"`
	}
	if err := json.Unmarshal(raw, &ep); err != nil {
		return err
	}
	network := "unix"
	if runtime.GOOS == "windows" {
		network = "tcp"
	}
	conn, err := net.DialTimeout(network, ep.Endpoint, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))

	inner, err := json.Marshal(map[string]any{"method": method, "params": params})
	if err != nil {
		return err
	}
	reqLine, err := json.Marshal(map[string]any{
		"token":  ep.Token,
		"method": "control.call",
		"params": json.RawMessage(inner),
	})
	if err != nil {
		return err
	}
	if _, err := conn.Write(append(reqLine, '\n')); err != nil {
		return err
	}
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 64<<10), 8<<20)
	if !sc.Scan() {
		if err := sc.Err(); err != nil {
			return err
		}
		return io.ErrUnexpectedEOF
	}
	var resp struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(sc.Bytes(), &resp); err != nil {
		return err
	}
	if resp.Error != nil {
		return fmt.Errorf("ipc %s error (%s): %s", method, resp.Error.Code, resp.Error.Message)
	}
	if resultOut != nil && len(resp.Result) > 0 {
		return json.Unmarshal(resp.Result, resultOut)
	}
	return nil
}

func (n *runningBCN) eventsSince(afterSeq int64) ([]auditEvent, error) {
	var resp struct {
		Events []auditEvent `json:"events"`
	}
	if err := n.adminCall("events.since", map[string]any{"afterSeq": afterSeq, "limit": 500}, &resp); err != nil {
		return nil, err
	}
	return resp.Events, nil
}

func (n *runningBCN) waitEvent(timeout time.Duration, afterSeq int64, match func(auditEvent) bool) (auditEvent, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		evs, err := n.eventsSince(afterSeq)
		if err == nil {
			for _, ev := range evs {
				if match(ev) {
					return ev, nil
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return auditEvent{}, errors.New("timed out waiting for matching audit event")
}

func (n *runningBCN) listPendingReviews(agentID string) ([]reviewConversation, error) {
	var resp struct {
		Reviews []reviewConversation `json:"reviews"`
	}
	if err := n.adminCall("reviews.list", map[string]any{"agent": agentID, "status": "pending"}, &resp); err != nil {
		return nil, err
	}
	return resp.Reviews, nil
}

func (n *runningBCN) dumpEventsToFile(path string) {
	evs, err := n.eventsSince(0)
	if err != nil {
		return
	}
	var buf bytes.Buffer
	for _, ev := range evs {
		b, _ := json.Marshal(ev)
		buf.Write(b)
		buf.WriteByte('\n')
	}
	_ = os.WriteFile(path, buf.Bytes(), 0o644)
}

func copyFixtureAttachment(t *testing.T, homeDir, agentID string) []byte {
	t.Helper()
	src := filepath.Join("fixtures", "harness", "attachment.txt")
	raw, err := os.ReadFile(src)
	if err != nil {
		t.Fatalf("read fixture attachment: %v", err)
	}
	wsDir := filepath.Join(homeDir, "workspaces", agentID)
	if err := os.MkdirAll(wsDir, 0o700); err != nil {
		t.Fatalf("mkdir agent workspace: %v", err)
	}
	if err := os.WriteFile(filepath.Join(wsDir, "attachment.txt"), raw, 0o600); err != nil {
		t.Fatalf("write workspace attachment: %v", err)
	}
	return raw
}

func writeLarkConfig(t *testing.T, homeDir, agentID, appID, appSecret, appSecretEnv, domain string) {
	t.Helper()
	var opts strings.Builder
	fmt.Fprintf(&opts, "appId = %q\n", appID)
	if appSecret != "" {
		fmt.Fprintf(&opts, "appSecret = %q\n", appSecret)
	}
	if appSecretEnv != "" {
		fmt.Fprintf(&opts, "appSecretEnv = %q\n", appSecretEnv)
	}
	if domain != "" {
		fmt.Fprintf(&opts, "domain = %q\n", domain)
	}
	cfg := fmt.Sprintf(`version = 1
revision = 1

[[agents]]
id = %q
name = "Lark Test"
mode = "dangerous_individual"
sandbox = "danger-full-access"
idle_timeout = 0

[[agents.channels]]
id = "channel-lark-1"
plugin = "channel-lark"

[agents.channels.options]
%s
[[agents.harnesses]]
id = "harness-e2e-echo-1"
plugin = "harness-e2e-echo"
`, agentID, opts.String())
	if err := os.WriteFile(filepath.Join(homeDir, "config.toml"), []byte(cfg), 0o600); err != nil {
		t.Fatalf("write config.toml: %v", err)
	}
}
