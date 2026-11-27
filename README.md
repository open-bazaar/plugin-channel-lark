# Bazaar Compute Lark / Feishu Channel Plugin

Official Lark / Feishu `channel` plugin (`channel-lark`) for Bazaar Compute 0.4 (`>= 0.4.0-rc.1`).

## Capabilities
- Native WebSocket long-connection over `bazaar.ws` with zero-dependency protobuf framing, `CONTROL` ping/pong `ClientConfig` updates, fragment reassembly & expiry pruning, ordered inbound event dispatch, event deduplication, and `bazaar.timers`-backed heartbeats & reconnection
- Singleflight `tenant_access_token` auto-refresh with `bazaar.secrets.register` redaction, per-request `bazaar.http` timeouts, invalidated-token retry (`99991663`), and 429 / `99991400` backoff
- Full 0.3 message & attachment parity: multi-locale `post` rich-text + embedded media extraction, `@_user_N` / `<at>` mention normalization, user & group directory lookup with TTL caching, silent quoted-reply backfill, Markdown `post` splitting with code-fence preservation, topic thread reply chaining, and typed media uploads
- Interactive approval cards using Card JSON 2.0 (`promptInteraction` / `cancelInteraction`), cryptographic callback tokens, fail-closed actor/chat/message validation, pre-ACK toast responses, and card redraw/close updates
- Read-marker emoji reaction (`Get`) via `setTyping`
- Unified `dm:${openId}` targets, `resolveDM` with `DMResolveContact` + KV fallback, canonical `conversationKey`, and `parentConversationKey` inheritance for group threads

## Configuration
- `appId` (required): Lark / Feishu application App ID (`cli_...`)
- `appSecret` (required, write-only): Lark / Feishu application App Secret
- `region` (optional): `"feishu"` (`https://open.feishu.cn`, default) or `"lark"` (`https://open.larksuite.com`)
- `domain` (optional): Custom API base URL override

## Install
```bash
bcn plugin install open-bazaar/plugin-channel-lark
```
