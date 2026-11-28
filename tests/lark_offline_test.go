package lark_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOfflinePackageAndInspect(t *testing.T) {
	bcnBin := resolveBCNBin(t)
	tmp := t.TempDir()
	zipPath := filepath.Join(tmp, "plugin.zip")
	entries := packagePluginDir(t, "..", zipPath)

	for _, name := range []string{"plugin.json", "index.lua", "content.lua", "frame.lua", "outbound.lua", "LICENSE"} {
		if !slices.Contains(entries, name) {
			t.Fatalf("packaged plugin.zip missing %q; entries=%v", name, entries)
		}
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry, "tests/") || strings.HasPrefix(entry, ".github/") {
			t.Fatalf("packaged plugin.zip leaked excluded file %q", entry)
		}
	}

	homeDir := filepath.Join(tmp, "home")
	installPluginZip(t, bcnBin, homeDir, zipPath)
	out := runBCN(t, bcnBin, homeDir, nil, "plugin", "list")
	if !strings.Contains(out, "channel-lark") || !strings.Contains(out, "enabled") {
		t.Fatalf("unexpected bcn plugin list output:\n%s", out)
	}
}

func TestOfflineMockLarkAuthAndWSSValidation(t *testing.T) {
	bcnBin := resolveBCNBin(t)
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	const mockSecret = "lark-mock-secret-never-leak"

	larkZip := filepath.Join(tmp, "channel-lark.zip")
	harnessZip := filepath.Join(tmp, "harness-e2e-echo.zip")
	packagePluginDir(t, "..", larkZip)
	packagePluginDir(t, filepath.Join("fixtures", "harness"), harnessZip)

	installPluginZip(t, bcnBin, homeDir, larkZip)
	installPluginZip(t, bcnBin, homeDir, harnessZip)
	patchReceiptOverrides(t, homeDir, "channel-lark", receiptOverrides{
		Network:      []string{"127.0.0.1"},
		PrivateHosts: []string{"127.0.0.1"},
	})
	patchReceiptOverrides(t, homeDir, "harness-e2e-echo", receiptOverrides{
		ExecOverrides: map[string]string{"bcn": bcnBin},
	})

	var sawToken atomic.Bool
	var sawBotInfo atomic.Bool
	var sawWsEndpoint atomic.Bool

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/open-apis/auth/v3/tenant_access_token/internal":
			var body map[string]any
			_ = json.NewDecoder(r.Body).Decode(&body)
			if body["app_id"] == "cli_mock_test" && body["app_secret"] == mockSecret {
				sawToken.Store(true)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code":                0,
				"tenant_access_token": "t-mock-token",
				"expire":              7200,
			})
		case "/open-apis/bot/v3/info":
			if r.Header.Get("Authorization") == "Bearer t-mock-token" {
				sawBotInfo.Store(true)
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"bot": map[string]any{
					"open_id":  "ou_mock_bot_1",
					"app_name": "Mock Lark Bot",
				},
			})
		case "/callback/ws/endpoint":
			sawWsEndpoint.Store(true)
			// Intentionally return insecure ws:// so plugin enforces wss:// and stays safe.
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0,
				"data": map[string]any{
					"URL": "ws://127.0.0.1:9999/ws?service_id=1&device_id=dev1",
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	// Exercise appSecretEnv resolution through real bcn + SpaceNet + MoeLua plugin.
	// Because the mock returns an insecure ws:// URL, channel-lark must fail closed on startup
	// after completing token + bot/v3/info + callback/ws/endpoint without leaking appSecret.
	writeLarkConfig(t, homeDir, "lark-mock-test", "cli_mock_test", "", "LARK_MOCK_SECRET", srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, bcnBin, "--home", homeDir, "run")
	cmd.Env = cleanEnv(map[string]string{
		"LARK_MOCK_SECRET": mockSecret,
	})
	rawOut, err := cmd.CombinedOutput()
	out := ansiRe.ReplaceAllString(string(rawOut), "")
	if err == nil {
		t.Fatalf("expected bcn run to fail closed on insecure ws:// endpoint, but exited 0:\n%s", out)
	}
	if !strings.Contains(out, "provider endpoint URL must use wss://") {
		t.Fatalf("expected wss:// enforcement error, got:\n%s", out)
	}
	if !sawToken.Load() || !sawBotInfo.Load() || !sawWsEndpoint.Load() {
		t.Fatalf("expected token/bot_info/ws_endpoint handshake calls (token=%v botInfo=%v ws=%v)\noutput:\n%s",
			sawToken.Load(), sawBotInfo.Load(), sawWsEndpoint.Load(), out)
	}
	if strings.Contains(out, mockSecret) {
		t.Fatalf("Lark appSecret leaked into bcn output")
	}
}
