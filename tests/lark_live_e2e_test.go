package lark_test

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	larkAPIBase     = "https://open.feishu.cn/open-apis/"
	liveWaitTimeout = 180 * time.Second
)

type larkApp struct {
	client *http.Client
	appID  string
	secret string
	token  string
}

func newLarkApp(appID, secret string) *larkApp {
	return &larkApp{
		client: &http.Client{Timeout: 45 * time.Second},
		appID:  appID,
		secret: secret,
	}
}

func (a *larkApp) ensureToken(ctx context.Context) (string, error) {
	if a.token != "" {
		return a.token, nil
	}
	body, _ := json.Marshal(map[string]string{"app_id": a.appID, "app_secret": a.secret})
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, larkAPIBase+"auth/v3/tenant_access_token/internal", bytes.NewReader(body))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var res struct {
		Code  int    `json:"code"`
		Msg   string `json:"msg"`
		Token string `json:"tenant_access_token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", err
	}
	if res.Code != 0 {
		return "", fmt.Errorf("lark token failed (%d): %s", res.Code, res.Msg)
	}
	a.token = res.Token
	return a.token, nil
}

func (a *larkApp) callJSON(ctx context.Context, method, relPath string, query url.Values, payload any) (map[string]any, error) {
	tok, err := a.ensureToken(ctx)
	if err != nil {
		return nil, err
	}
	endpoint := larkAPIBase + relPath
	if len(query) > 0 {
		endpoint += "?" + query.Encode()
	}
	var body io.Reader
	if payload != nil {
		b, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var env struct {
		Code int            `json:"code"`
		Msg  string         `json:"msg"`
		Data map[string]any `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, err
	}
	if env.Code != 0 {
		return nil, fmt.Errorf("lark %s failed (%d): %s", relPath, env.Code, env.Msg)
	}
	return env.Data, nil
}

func (a *larkApp) send(ctx context.Context, chatID, msgType string, content any) (map[string]any, error) {
	contentJSON, err := json.Marshal(content)
	if err != nil {
		return nil, err
	}
	q := url.Values{"receive_id_type": {"chat_id"}}
	return a.callJSON(ctx, http.MethodPost, "im/v1/messages", q, map[string]any{
		"receive_id": chatID,
		"msg_type":   msgType,
		"content":    string(contentJSON),
	})
}

func (a *larkApp) reply(ctx context.Context, messageID, text string, inThread bool) (map[string]any, error) {
	contentJSON, _ := json.Marshal(map[string]string{"text": text})
	return a.callJSON(ctx, http.MethodPost, "im/v1/messages/"+messageID+"/reply", nil, map[string]any{
		"msg_type":        "text",
		"content":         string(contentJSON),
		"reply_in_thread": inThread,
	})
}

func (a *larkApp) messages(ctx context.Context, containerType, containerID string) ([]map[string]any, error) {
	q := url.Values{
		"container_id_type": {containerType},
		"container_id":      {containerID},
		"sort_type":         {"ByCreateTimeDesc"},
		"page_size":         {"50"},
	}
	data, err := a.callJSON(ctx, http.MethodGet, "im/v1/messages", q, nil)
	if err != nil {
		return nil, err
	}
	rawItems, _ := data["items"].([]any)
	items := make([]map[string]any, 0, len(rawItems))
	for i := len(rawItems) - 1; i >= 0; i-- {
		if m, ok := rawItems[i].(map[string]any); ok {
			items = append(items, m)
		}
	}
	return items, nil
}

func (a *larkApp) uploadImage(ctx context.Context, pngBytes []byte) (string, error) {
	tok, err := a.ensureToken(ctx)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("image_type", "message")
	fw, err := mw.CreateFormFile("image", "echo.png")
	if err != nil {
		return "", err
	}
	_, _ = fw.Write(pngBytes)
	_ = mw.Close()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, larkAPIBase+"im/v1/images", &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := a.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var env struct {
		Code int `json:"code"`
		Msg  string `json:"msg"`
		Data struct {
			ImageKey string `json:"image_key"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return "", err
	}
	if env.Code != 0 {
		return "", fmt.Errorf("uploadImage failed (%d): %s", env.Code, env.Msg)
	}
	return env.Data.ImageKey, nil
}

func (a *larkApp) downloadResource(ctx context.Context, msg map[string]any, kind string) ([]byte, error) {
	tok, err := a.ensureToken(ctx)
	if err != nil {
		return nil, err
	}
	bodyMap, _ := msg["body"].(map[string]any)
	rawContent, _ := bodyMap["content"].(string)
	var contentObj map[string]any
	if err := json.Unmarshal([]byte(rawContent), &contentObj); err != nil {
		return nil, err
	}
	key, _ := contentObj[kind+"_key"].(string)
	msgID, _ := msg["message_id"].(string)
	endpoint := fmt.Sprintf("%sim/v1/messages/%s/resources/%s?type=%s", larkAPIBase, msgID, key, url.QueryEscape(kind))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("downloadResource status %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func buildDeterministicPNG(seed []byte) []byte {
	const size = 32
	var rows bytes.Buffer
	for y := 0; y < size; y++ {
		rows.WriteByte(0)
		for x := 0; x < size*3; x++ {
			rows.WriteByte(seed[(x+y)%len(seed)])
		}
	}
	var compressed bytes.Buffer
	zw := zlib.NewWriter(&compressed)
	_, _ = zw.Write(rows.Bytes())
	_ = zw.Close()

	chunk := func(kind string, data []byte) []byte {
		var out bytes.Buffer
		_ = binary.Write(&out, binary.BigEndian, uint32(len(data)))
		out.WriteString(kind)
		out.Write(data)
		h := crc32.NewIEEE()
		_, _ = h.Write([]byte(kind))
		_, _ = h.Write(data)
		_ = binary.Write(&out, binary.BigEndian, h.Sum32())
		return out.Bytes()
	}

	var ihdr bytes.Buffer
	_ = binary.Write(&ihdr, binary.BigEndian, uint32(size))
	_ = binary.Write(&ihdr, binary.BigEndian, uint32(size))
	ihdr.Write([]byte{8, 2, 0, 0, 0})

	var png bytes.Buffer
	png.Write([]byte{0x89, 'P', 'N', 'G', '\r', '\n', 0x1a, '\n'})
	png.Write(chunk("IHDR", ihdr.Bytes()))
	png.Write(chunk("IDAT", compressed.Bytes()))
	png.Write(chunk("IEND", nil))
	return png.Bytes()
}

func renderedLarkPost(item map[string]any) string {
	bodyMap, _ := item["body"].(map[string]any)
	raw, _ := bodyMap["content"].(string)
	var post struct {
		Content [][]struct {
			Text string `json:"text"`
		} `json:"content"`
	}
	if err := json.Unmarshal([]byte(raw), &post); err != nil {
		return raw
	}
	lines := make([]string, 0, len(post.Content))
	for _, row := range post.Content {
		var sb strings.Builder
		for _, node := range row {
			sb.WriteString(node.Text)
		}
		lines = append(lines, sb.String())
	}
	return strings.Join(lines, "\n")
}

func randomMarker(t *testing.T) string {
	t.Helper()
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	return "e2e-" + hex.EncodeToString(b[:])
}

func TestLiveLarkE2E(t *testing.T) {
	testedID := strings.TrimSpace(os.Getenv("LARK_TEST_APP_ID"))
	testedSecret := strings.TrimSpace(os.Getenv("LARK_TEST_APP_SECRET"))
	driverID := strings.TrimSpace(os.Getenv("LARK_DRIVER_APP_ID"))
	driverSecret := strings.TrimSpace(os.Getenv("LARK_DRIVER_APP_SECRET"))
	chatID := strings.TrimSpace(os.Getenv("LARK_TEST_CHAT_ID"))
	if testedID == "" || testedSecret == "" || driverID == "" || driverSecret == "" || chatID == "" {
		if os.Getenv("BCN_REQUIRE_LIVE_E2E") == "1" {
			t.Fatal("missing required LARK_TEST_APP_ID / LARK_TEST_APP_SECRET / LARK_DRIVER_APP_ID / LARK_DRIVER_APP_SECRET / LARK_TEST_CHAT_ID")
		}
		t.Skip("live Lark E2E secrets/vars not configured")
	}

	bcnBin := resolveBCNBin(t)
	artDir := artifactDir(t, "lark")
	tmp := t.TempDir()
	homeDir := filepath.Join(tmp, "home")
	const agentID = "lark-test"

	larkZip := filepath.Join(tmp, "channel-lark.zip")
	harnessZip := filepath.Join(tmp, "harness-e2e-echo.zip")
	packagePluginDir(t, "..", larkZip)
	packagePluginDir(t, filepath.Join("fixtures", "harness"), harnessZip)

	installPluginZip(t, bcnBin, homeDir, larkZip)
	installPluginZip(t, bcnBin, homeDir, harnessZip)
	patchReceiptOverrides(t, homeDir, "harness-e2e-echo", receiptOverrides{
		ExecOverrides: map[string]string{"bcn": bcnBin},
	})
	expectedAttachment := copyFixtureAttachment(t, homeDir, agentID)

	driver := newLarkApp(driverID, driverSecret)
	ctx := context.Background()

	var activeNode *runningBCN
	t.Cleanup(func() {
		if activeNode != nil {
			activeNode.dumpEventsToFile(filepath.Join(artDir, "audit.jsonl"))
			_ = activeNode.Stop()
		}
	})

	answers := func(text string) func(map[string]any) bool {
		return func(item map[string]any) bool {
			sender, _ := item["sender"].(map[string]any)
			body, _ := item["body"].(map[string]any)
			content, _ := body["content"].(string)
			return fmt.Sprint(sender["id"]) == testedID && strings.Contains(content, text)
		}
	}
	sentAfter := func(asked map[string]any, msgType string) func(map[string]any) bool {
		askedTS, _ := strconv.ParseInt(fmt.Sprint(asked["create_time"]), 10, 64)
		return func(item map[string]any) bool {
			sender, _ := item["sender"].(map[string]any)
			ts, _ := strconv.ParseInt(fmt.Sprint(item["create_time"]), 10, 64)
			return fmt.Sprint(sender["id"]) == testedID && fmt.Sprint(item["msg_type"]) == msgType && ts >= askedTS
		}
	}
	waitPosted := func(threadID string, match func(map[string]any) bool) map[string]any {
		t.Helper()
		containerType := "chat"
		containerID := chatID
		if threadID != "" {
			containerType = "thread"
			containerID = threadID
		}
		deadline := time.Now().Add(liveWaitTimeout)
		for time.Now().Before(deadline) {
			items, err := driver.messages(ctx, containerType, containerID)
			if err == nil {
				for _, item := range items {
					if match(item) {
						return item
					}
				}
			}
			time.Sleep(2 * time.Second)
		}
		t.Fatalf("timed out waiting for Lark message in %s %s", containerType, containerID)
		return nil
	}

	// Start with inline appSecret.
	writeLarkConfig(t, homeDir, agentID, testedID, testedSecret, "", "")
	activeNode = startBCNNode(t, bcnBin, homeDir, nil, filepath.Join(artDir, "node-inline.log"))

	// 1. Verify connected event + identity.
	connected, err := activeNode.waitEvent(60*time.Second, 0, func(ev auditEvent) bool {
		return ev.Type == "channel.connected" && ev.PluginID == "channel-lark"
	})
	if err != nil {
		t.Fatalf("wait channel.connected: %v\nlog:\n%s", err, activeNode.LogText())
	}
	firstConnectedAt := time.Now()
	idMap, _ := connected.Payload["identity"].(map[string]any)
	if fmt.Sprint(idMap["id"]) == "" {
		t.Fatalf("expected non-empty Lark bot identity: %+v", idMap)
	}

	// 2. Open conversation -> pending review -> approve via bcn CLI.
	openMarker := randomMarker(t)
	if _, err := driver.send(ctx, chatID, "text", map[string]string{"text": openMarker + " open"}); err != nil {
		t.Fatalf("driver send open: %v", err)
	}
	var reviewConv reviewConversation
	revDeadline := time.Now().Add(liveWaitTimeout)
	for time.Now().Before(revDeadline) {
		revs, _ := activeNode.listPendingReviews(agentID)
		for _, r := range revs {
			if r.Target == "group:"+chatID {
				reviewConv = r
				break
			}
		}
		if reviewConv.ID != "" {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if reviewConv.ID == "" {
		t.Fatalf("timed out waiting for pending Lark review for group:%s", chatID)
	}
	runBCN(t, bcnBin, homeDir, nil, "agent", "review", "approve", reviewConv.ID)

	// 3. Direct reply check (parent_id matches asked message_id).
	rMarker := randomMarker(t)
	asked, err := driver.send(ctx, chatID, "text", map[string]string{"text": rMarker + " reply"})
	if err != nil {
		t.Fatalf("driver send reply: %v", err)
	}
	ans := waitPosted("", answers(rMarker+" reply"))
	if fmt.Sprint(ans["parent_id"]) != fmt.Sprint(asked["message_id"]) {
		t.Fatalf("expected parent_id=%v, got %v", asked["message_id"], ans["parent_id"])
	}

	// 4. Quoted message backfill.
	qMarker := randomMarker(t)
	original, err := driver.send(ctx, chatID, "text", map[string]string{"text": qMarker + " original"})
	if err != nil {
		t.Fatalf("driver send original: %v", err)
	}
	if _, err := driver.reply(ctx, fmt.Sprint(original["message_id"]), qMarker+" quote", false); err != nil {
		t.Fatalf("driver reply quote: %v", err)
	}
	waitPosted("", answers(qMarker+" original"))

	// 5. Long multi-part chunking.
	lMarker := randomMarker(t)
	if _, err := driver.send(ctx, chatID, "text", map[string]string{"text": lMarker + " long"}); err != nil {
		t.Fatalf("driver send long: %v", err)
	}
	waitPosted("", answers("END "+lMarker+" 80"))
	items, err := driver.messages(ctx, "chat", chatID)
	if err != nil {
		t.Fatalf("list chat messages: %v", err)
	}
	secRe := regexp.MustCompile(regexp.QuoteMeta(lMarker) + `-(\d{4})`)
	var parts []string
	var sections []int
	for _, it := range items {
		if answers(lMarker)(it) {
			txt := renderedLarkPost(it)
			parts = append(parts, txt)
			for _, sub := range secRe.FindAllStringSubmatch(txt, -1) {
				n, _ := strconv.Atoi(sub[1])
				sections = append(sections, n)
			}
		}
	}
	if len(parts) < 2 {
		t.Fatalf("expected multiple Lark messages for long reply, got %d", len(parts))
	}
	if len(sections) != 80 {
		t.Fatalf("expected 80 sections across chunks, got %d", len(sections))
	}
	for i, n := range sections {
		if n != i+1 {
			t.Fatalf("section %d out of order: got %d", i+1, n)
		}
	}

	// 6. Outbound file attachment.
	aMarker := randomMarker(t)
	attAsked, err := driver.send(ctx, chatID, "text", map[string]string{"text": aMarker + " attach"})
	if err != nil {
		t.Fatalf("driver send attach: %v", err)
	}
	attached := waitPosted("", sentAfter(attAsked, "file"))
	gotAtt, err := driver.downloadResource(ctx, attached, "file")
	if err != nil {
		t.Fatalf("download outbound file: %v", err)
	}
	if !bytes.Equal(gotAtt, expectedAttachment) {
		t.Fatalf("outbound file attachment bytes mismatch")
	}

	// 7. Inbound PNG inside rich post + outbound image echo.
	eMarker := randomMarker(t)
	seed := make([]byte, 64)
	_, _ = rand.Read(seed)
	pngBytes := buildDeterministicPNG(seed)
	imgKey, err := driver.uploadImage(ctx, pngBytes)
	if err != nil {
		t.Fatalf("uploadImage: %v", err)
	}
	imgAsked, err := driver.send(ctx, chatID, "post", map[string]any{
		"zh_cn": map[string]any{
			"title": "",
			"content": [][]map[string]any{
				{{"tag": "text", "text": eMarker + " echo"}},
				{{"tag": "img", "image_key": imgKey}},
			},
		},
	})
	if err != nil {
		t.Fatalf("driver send post image: %v", err)
	}
	echoed := waitPosted("", sentAfter(imgAsked, "image"))
	gotPNG, err := driver.downloadResource(ctx, echoed, "image")
	if err != nil {
		t.Fatalf("download echoed image: %v", err)
	}
	if !bytes.Equal(gotPNG, pngBytes) {
		t.Fatalf("echoed PNG bytes mismatch")
	}

	// 8. Thread reply + parent approval inheritance.
	tMarker := randomMarker(t)
	rootMsg, err := driver.send(ctx, chatID, "text", map[string]string{"text": tMarker + " root"})
	if err != nil {
		t.Fatalf("driver send root: %v", err)
	}
	threadAsked, err := driver.reply(ctx, fmt.Sprint(rootMsg["message_id"]), tMarker+" reply", true)
	if err != nil {
		t.Fatalf("driver reply in_thread: %v", err)
	}
	threadID := fmt.Sprint(threadAsked["thread_id"])
	if threadID == "" {
		t.Fatalf("reply_in_thread did not return thread_id: %+v", threadAsked)
	}
	waitPosted(threadID, answers(tMarker+" reply"))

	// 9. Linux socket-drop WebSocket reconnect recovery (mirrors 0.3 CI ss -K check).
	if runtime.GOOS == "linux" {
		if waitRem := 60*time.Second - time.Since(firstConnectedAt); waitRem > 0 {
			time.Sleep(waitRem)
		}
		if ssOut, ssErr := exec.Command("ss", "-Hntp").Output(); ssErr == nil {
			pidTag := fmt.Sprintf("pid=%d,", activeNode.PID())
			var endpoints [][2]string
			for _, line := range strings.Split(string(ssOut), "\n") {
				fields := strings.Fields(line)
				if len(fields) >= 5 && strings.Contains(line, pidTag) && strings.HasSuffix(fields[4], ":443") {
					endpoints = append(endpoints, [2]string{fields[3], fields[4]})
				}
			}
			if len(endpoints) > 0 {
				evsBefore, _ := activeNode.eventsSince(0)
				var lastSeq int64
				if len(evsBefore) > 0 {
					lastSeq = evsBefore[len(evsBefore)-1].Seq
				}
				droppedAny := false
				for _, ep := range endpoints {
					if err := exec.Command("sudo", "-n", "ss", "-K", "src", ep[0], "dst", ep[1]).Run(); err == nil {
						droppedAny = true
					}
				}
				if droppedAny {
					if _, err := activeNode.waitEvent(liveWaitTimeout, lastSeq, func(ev auditEvent) bool {
						return ev.Type == "channel.connected" && ev.PluginID == "channel-lark"
					}); err != nil {
						t.Fatalf("Lark WS did not reconnect after socket drop: %v", err)
					}
					recMarker := randomMarker(t)
					recAsked, err := driver.send(ctx, chatID, "text", map[string]string{"text": recMarker + " reply"})
					if err != nil {
						t.Fatalf("post-reconnect send: %v", err)
					}
					recAns := waitPosted("", answers(recMarker+" reply"))
					if fmt.Sprint(recAns["parent_id"]) != fmt.Sprint(recAsked["message_id"]) {
						t.Fatalf("post-reconnect reply parent mismatch")
					}
				}
			}
		}
	}

	// 10. Restart with appSecretEnv (no inline appSecret) + zero secret leak check.
	activeNode.dumpEventsToFile(filepath.Join(artDir, "audit-inline.jsonl"))
	_ = activeNode.Stop()
	if strings.Contains(activeNode.LogText(), testedSecret) {
		t.Fatalf("Lark appSecret leaked into node-inline.log")
	}
	activeNode = nil

	writeLarkConfig(t, homeDir, agentID, testedID, "", "LARK_TEST_APP_SECRET", "")
	activeNode = startBCNNode(t, bcnBin, homeDir, map[string]string{
		"LARK_TEST_APP_SECRET": testedSecret,
	}, filepath.Join(artDir, "node-env.log"))
	if _, err := activeNode.waitEvent(60*time.Second, 0, func(ev auditEvent) bool {
		return ev.Type == "channel.connected" && ev.PluginID == "channel-lark"
	}); err != nil {
		t.Fatalf("wait channel.connected after appSecretEnv restart: %v", err)
	}
	envMarker := randomMarker(t)
	envAsked, err := driver.send(ctx, chatID, "text", map[string]string{"text": envMarker + " reply"})
	if err != nil {
		t.Fatalf("appSecretEnv send: %v", err)
	}
	envAns := waitPosted("", answers(envMarker+" reply"))
	if fmt.Sprint(envAns["parent_id"]) != fmt.Sprint(envAsked["message_id"]) {
		t.Fatalf("appSecretEnv reply parent mismatch")
	}
	if strings.Contains(activeNode.LogText(), testedSecret) {
		t.Fatalf("Lark appSecret leaked into node-env.log")
	}
}
