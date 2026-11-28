-- Official Lark / Feishu WebSocket + OpenAPI channel plugin for Bazaar Compute 0.4 (Lua 5.5).

local task = require("bazaar.task")
local timers = require("bazaar.timers")
local http = require("bazaar.http")
local wsClient = require("bazaar.ws")
local json = require("bazaar.json")
local urlMod = require("bazaar.url")
local time = require("bazaar.time")
local crypto = require("bazaar.crypto")
local log = require("bazaar.log")
local secrets = require("bazaar.secrets")
local kv = require("bazaar.kv")
local hasAttachments, loadedAttachments = pcall(function()
  return require("bazaar.attachments")
end)
local attachmentsMod = hasAttachments and loadedAttachments or nil

local frameMod = require("./frame")
local contentMod = require("./content")
local outboundMod = require("./outbound")

local newFrameDecodeError = frameMod.newFrameDecodeError
local isFrameDecodeError = frameMod.isFrameDecodeError
local decodeFrame = frameMod.decodeFrame
local encodeFrame = frameMod.encodeFrame
local getHeaderValues = frameMod.getHeaderValues

local DOWNLOAD_TYPES = contentMod.DOWNLOAD_TYPES
local fallbackContentType = contentMod.fallbackContentType
local isArray = contentMod.isArray
local isRecord = contentMod.isRecord
local parseMentions = contentMod.parseMentions
local projectContent = contentMod.projectContent
local providerText = contentMod.providerText
local providerTimeMs = contentMod.providerTimeMs
local resourceName = contentMod.resourceName

local newLarkApiError = outboundMod.newLarkApiError
local newLarkTransportError = outboundMod.newLarkTransportError
local isLarkApiError = outboundMod.isLarkApiError
local MAX_MARKDOWN_CODEPOINTS = outboundMod.MAX_MARKDOWN_CODEPOINTS
local isAmbiguousDeliveryError = outboundMod.isAmbiguousDeliveryError
local isFatalAuthError = outboundMod.isFatalAuthError
local isRateLimitError = outboundMod.isRateLimitError
local isTokenInvalidError = outboundMod.isTokenInvalidError
local markdownPostContent = outboundMod.markdownPostContent
local splitMarkdown = outboundMod.splitMarkdown
local validateAttachmentMeta = outboundMod.validateAttachmentMeta

local CONTROL_METHOD = 0
local DATA_METHOD = 1
local HEADER_TYPE = "type"
local HEADER_MESSAGE_ID = "message_id"
local HEADER_SUM = "sum"
local HEADER_SEQ = "seq"
local HEADER_BIZ_RT = "biz_rt"
local MESSAGE_EVENT = "event"
local MESSAGE_CARD = "card"
local MESSAGE_PING = "ping"
local MESSAGE_PONG = "pong"

local REGION_BASE_URLS = {
  feishu = "https://open.feishu.cn",
  lark = "https://open.larksuite.com",
}

local CARD_EVENT_TYPES = {
  ["card.action.trigger"] = true,
  ["p2.card.action.trigger"] = true,
}
local VALID_CHAT_TYPES = {
  p2p = true,
  group = true,
  topic = true,
}
local AGENT_SENDER_TYPES = {
  app = true,
  bot = true,
  system = true,
}

local API_TIMEOUT_MS = 30000
local MEDIA_TIMEOUT_MS = 60000
local CALL_TIMEOUT_MS = 10000
local CARD_UPDATE_TIMEOUT_MS = 10000
local TOKEN_REFRESH_MARGIN_MS = 600000
local FRAGMENT_EXPIRY_MS = 5000
local WS_HEARTBEAT_INTERVAL_MS = 30000
local STARTUP_RETRY_MS = 250
local STARTUP_CONNECT_BUDGET_MS = 15000
local DISCONNECT_CLOSE_BUDGET_MS = 5000
local NAME_HIT_TTL_MS = 24 * 60 * 60 * 1000
local NAME_MISS_TTL_MS = 5 * 60 * 1000
local MAX_DEDUP_EVENTS = 2048
local MAX_RATE_LIMIT_RETRIES = 3
local MAX_DELIVERY_RETRIES = 2
local READ_MARKER_EMOJI = "Get"

local KV_DM_HANDLE_PREFIX = "lark:dm:"
local KV_DM_CHAT_PREFIX = "lark:dm_chat:"
local KV_TOPIC_ROOT_PREFIX = "lark:topic_root:"

local MONTH_MAP = {
  jan = 1, feb = 2, mar = 3, apr = 4, may = 5, jun = 6,
  jul = 7, aug = 8, sep = 9, oct = 10, nov = 11, dec = 12,
}

-- Connection & configuration state
local apiBase = REGION_BASE_URLS.feishu
local regionKey = "feishu"
local appId = ""
local appSecret = ""
local cachedToken = ""
local tokenRefreshAt = 0
local tokenRefreshToken = nil
local botIdentity = nil
local emitFn = nil
local stopping = false

-- Transport state
local wsHandle = nil
local activeEndpoint = nil
local reconnectTimer = nil
local reconnectBackoffMs = 1000
local reconnectAttempts = 0
local wsGeneration = 0

-- Caches & queues
local fragments = {} -- message_id -> { total, updatedMs, count, parts = { [seq] = bytes } }
local dedupNodes = {} -- event_id -> { key, prev, next }
local dedupHead = nil
local dedupTail = nil
local dedupSize = 0
local senderNameCache = {} -- open_id -> { expiresAt, name }
local groupNameCache = {} -- chat_id -> { expiresAt, name }
local dmChatIds = {} -- peer_open_id -> oc_chat_id
local topicRoots = {} -- thread_id -> root/anchor message_id

-- Ordered inbound event pipeline (bazaar.task.channel FIFO worker)
local eventChannel = nil

-- Interactive approval card tracking
local pendingByToken = {} -- callback token -> entry
local pendingByPromptId = {} -- promptId -> entry
local activeCardTasks = {} -- { [tracker] = true }

-- ---------------- Timer & Utility Helpers ----------------

local function isNull(v)
  return v == nil or (json.null ~= nil and v == json.null)
end

local function trim(s)
  if isNull(s) then
    return ""
  end
  return (tostring(s):gsub("^%s+", ""):gsub("%s+$", ""))
end

local function isFiniteNumber(v)
  return type(v) == "number" and v == v and v ~= math.huge and v ~= -math.huge
end

local function isIntegerNumber(v)
  return isFiniteNumber(v) and (math.type(v) == "integer" or math.floor(v) == v)
end

local function wallNowMs()
  return time.nowMs()
end

local function monoNowMs()
  if timers.nowMs then
    return timers.nowMs()
  end
  return time.nowMs()
end

local function clampDelayMs(ms)
  local maxDelay = (isFiniteNumber(timers.maxDelayMs) and timers.maxDelayMs > 0)
      and math.floor(timers.maxDelayMs)
    or 2147483647
  local n = tonumber(ms)
  if not isFiniteNumber(n) or n <= 0 then
    return 0
  end
  local ceiled = math.ceil(n)
  if ceiled > maxDelay then
    return maxDelay
  end
  return ceiled
end

local function sleepMs(ms)
  local delay = clampDelayMs(ms)
  if delay <= 0 then
    return
  end
  task.sleep(delay)
end

local function cancelTimer(timer)
  if not timer then
    return
  end
  pcall(timer.cancel, timer)
end

local function errMessage(err)
  if type(err) == "table" and err.message ~= nil then
    return tostring(err.message)
  end
  return tostring(err or "")
end

local function replaceLiteral(str, needle, replacement)
  if needle == "" then
    return str
  end
  local parts = nil
  local startPos = 1
  local needleLen = #needle
  while true do
    local i = string.find(str, needle, startPos, true)
    if not i then
      if not parts then
        return str
      end
      parts[#parts + 1] = string.sub(str, startPos)
      return table.concat(parts)
    end
    if not parts then
      parts = {}
    end
    parts[#parts + 1] = string.sub(str, startPos, i - 1)
    parts[#parts + 1] = replacement
    startPos = i + needleLen
  end
end

local function registerSecret(val)
  if type(val) ~= "string" or #trim(val) < 4 then
    return
  end
  pcall(function()
    if secrets.register then
      secrets.register(val)
    end
  end)
end

local function scrubSecrets(text)
  local out = tostring(text or "")
  for _, secret in ipairs({ appSecret, cachedToken }) do
    if type(secret) == "string" and #secret >= 4 and string.find(out, secret, 1, true) ~= nil then
      out = replaceLiteral(out, secret, "[REDACTED]")
    end
  end
  return out
end

local function encodeComponentStrict(raw)
  -- bazaar.url.encodeComponent uses Go QueryEscape (spaces as '+'); replace '+' with '%20'
  -- so URL path segments match RFC 3986 / JS encodeURIComponent semantics.
  return (urlMod.encodeComponent(tostring(raw)):gsub("%+", "%%20"))
end

local function randomBase64Url(numBytes)
  local bytes = crypto.randomBytes(numBytes)
  local b64 = crypto.base64Encode(bytes)
  return (b64:gsub("%+", "-"):gsub("/", "_"):gsub("=+$", ""))
end

local function randomIntBelow(cap)
  if cap <= 0 then
    return 0
  end
  local b1, b2, b3, b4 = string.byte(crypto.randomBytes(4), 1, 4)
  local u32 = ((b1 << 24) | (b2 << 16) | (b3 << 8) | b4)
  return u32 % cap
end

local function generateUuid()
  local raw = crypto.randomBytes(16)
  local b = { string.byte(raw, 1, 16) }
  b[7] = (b[7] & 0x0f) | 0x40
  b[9] = (b[9] & 0x3f) | 0x80
  local patched = string.char(table.unpack(b))
  local hex = crypto.hexEncode(patched)
  return string.sub(hex, 1, 8)
    .. "-" .. string.sub(hex, 9, 12)
    .. "-" .. string.sub(hex, 13, 16)
    .. "-" .. string.sub(hex, 17, 20)
    .. "-" .. string.sub(hex, 21, 32)
end

local function kvGet(key)
  local ok, val = pcall(function()
    return kv.get(key)
  end)
  if ok then
    return val
  end
  return nil
end

local function kvSet(key, value)
  pcall(function()
    kv.set(key, tostring(value))
  end)
end

local function kvSetAsync(key, value)
  task.fork(kvSet, key, value)
end

local function daysFromCivil(y, m, d)
  if m <= 2 then
    y = y - 1
  end
  local era = (y >= 0 and y or (y - 399)) // 400
  local yoe = y - era * 400
  local mp = m + (m > 2 and -3 or 9)
  local doy = (153 * mp + 2) // 5 + d - 1
  local doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
  return era * 146097 + doe - 719468
end

local function parseHttpDateMs(str)
  -- IMF-fixdate / RFC 1123: "Wed, 21 Oct 2015 07:28:00 GMT"
  local d, mon, y, hh, mm, ss = str:match("^%a+,%s+(%d%d?)%s+(%a+)%s+(%d%d%d%d)%s+(%d%d):(%d%d):(%d%d)%s+GMT$")
  if not d then
    -- RFC 850: "Sunday, 06-Nov-94 08:49:37 GMT"
    d, mon, y, hh, mm, ss = str:match("^%a+,%s+(%d%d?)%-(%a+)%-(%d%d)%s+(%d%d):(%d%d):(%d%d)%s+GMT$")
    if y then
      local yy = tonumber(y)
      y = yy >= 70 and (1900 + yy) or (2000 + yy)
    end
  end
  if not d then
    -- ANSI C asctime: "Sun Nov  6 08:49:37 1994"
    mon, d, hh, mm, ss, y = str:match("^%a+%s+(%a+)%s+(%d%d?)%s+(%d%d):(%d%d):(%d%d)%s+(%d%d%d%d)$")
  end
  if not d then
    return nil
  end
  local mNum = MONTH_MAP[string.lower(mon)]
  local yNum, dNum = tonumber(y), tonumber(d)
  local hNum, minNum, sNum = tonumber(hh), tonumber(mm), tonumber(ss)
  if not (mNum and yNum and dNum and hNum and minNum and sNum) then
    return nil
  end
  local days = daysFromCivil(yNum, mNum, dNum)
  return (days * 86400 + hNum * 3600 + minNum * 60 + sNum) * 1000
end

local function getAllQueryParams(rawQuery, key)
  local out = {}
  if type(rawQuery) ~= "string" or rawQuery == "" then
    return out
  end
  for part in string.gmatch(rawQuery, "[^&]+") do
    local eq = string.find(part, "=", 1, true)
    local rawK, rawV
    if eq then
      rawK = string.sub(part, 1, eq - 1)
      rawV = string.sub(part, eq + 1)
    else
      rawK = part
      rawV = ""
    end
    local okK, decK = pcall(urlMod.decodeComponent, rawK)
    if okK and decK == key then
      local okV, decV = pcall(urlMod.decodeComponent, rawV)
      out[#out + 1] = okV and decV or rawV
    end
  end
  return out
end

-- ---------------- Region, Target & Conversation Key ----------------

local function deriveRegionKey(baseUrl, configuredRegion)
  local ok, parsed = pcall(urlMod.parse, baseUrl)
  if ok and type(parsed) == "table" and type(parsed.hostname) == "string" and parsed.hostname ~= "" then
    local hostname = string.lower(parsed.hostname)
    if hostname == "open.larksuite.com" or string.sub(hostname, -14) == ".larksuite.com" then
      return "lark"
    end
    if hostname == "open.feishu.cn" or string.sub(hostname, -10) == ".feishu.cn" then
      return "feishu"
    end
    return (configuredRegion ~= "" and configuredRegion) or hostname
  end
  return (configuredRegion ~= "" and configuredRegion) or "feishu"
end

local function resolveRegionAndDomain(config)
  local cfg = type(config) == "table" and config or {}
  local rawRegion = string.lower(trim(cfg.region))
  local rawDomain = trim(cfg.domain):gsub("/+$", "")

  if rawRegion ~= "" and not REGION_BASE_URLS[rawRegion] then
    error('lark: unsupported region "' .. rawRegion .. '" (expected "feishu" or "lark")', 0)
  end

  local resolvedBase = ""
  if rawDomain ~= "" then
    if rawRegion == "lark" and rawDomain == REGION_BASE_URLS.feishu then
      -- Handle UI forms that populate default domain alongside explicit region="lark".
      resolvedBase = REGION_BASE_URLS.lark
    else
      local ok, parsed = pcall(urlMod.parse, rawDomain)
      local scheme = (ok and type(parsed) == "table" and type(parsed.scheme) == "string")
          and string.lower(parsed.scheme)
        or ""
      local hostname = (ok and type(parsed) == "table" and type(parsed.hostname) == "string")
          and parsed.hostname
        or ""
      if not ok or hostname == "" then
        error('lark: invalid domain URL "' .. rawDomain .. '"', 0)
      end
      if scheme ~= "https" and scheme ~= "http" then
        error('lark: domain must use http or https scheme: "' .. rawDomain .. '"', 0)
      end
      resolvedBase = rawDomain
    end
  elseif rawRegion ~= "" then
    resolvedBase = REGION_BASE_URLS[rawRegion]
  else
    resolvedBase = REGION_BASE_URLS.feishu
  end

  return {
    apiBase = resolvedBase,
    regionKey = deriveRegionKey(resolvedBase, rawRegion),
  }
end

local function decodePercentSegment(s)
  if type(s) ~= "string" then
    return ""
  end
  return (s:gsub("%%(%x%x)", function(hex)
    return string.char(tonumber(hex, 16))
  end))
end

local function providerMessageAnchor(value)
  local txt = providerText(value)
  if not txt or string.sub(txt, 1, 6) == "group:" then
    return nil
  end
  return txt
end

local function parseTarget(target)
  local raw = providerText(trim(target))
  if not raw then
    error("lark: invalid or empty target", 0)
  end
  -- 0.3 canonical provider_thread_id format: lark:bot:<botOpenId>:chat:<chatOrOpenId>:thread:<threadId>
  if string.sub(raw, 1, 9) == "lark:bot:" then
    local rawBotId, rawChatOrOpen, rawThreadId = string.match(raw, "^lark:bot:([^:]+):chat:([^:]+):thread:([^:]+)$")
    if rawBotId and rawChatOrOpen and rawThreadId then
      local botId = providerText(decodePercentSegment(rawBotId))
      local cleanChat = providerText(decodePercentSegment(rawChatOrOpen))
      local cleanThread = providerText(decodePercentSegment(rawThreadId))
      if botId and cleanChat and cleanThread then
        local isOpenId = string.sub(cleanChat, 1, 3) == "ou_"
        if isOpenId then
          return {
            kind = "dm",
            receiveId = cleanChat,
            receiveIdType = "open_id",
            chatId = cleanChat,
            openId = cleanChat,
            threadId = "",
            botId = botId,
          }
        end
        if cleanThread ~= "0" then
          return {
            kind = "thread",
            receiveId = cleanChat,
            receiveIdType = "chat_id",
            chatId = cleanChat,
            openId = "",
            threadId = cleanThread,
            botId = botId,
          }
        end
        return {
          kind = "group",
          receiveId = cleanChat,
          receiveIdType = "chat_id",
          chatId = cleanChat,
          openId = "",
          threadId = "",
          botId = botId,
        }
      end
    end
    error('lark: invalid target "' .. raw .. '"', 0)
  end
  if string.sub(raw, 1, 3) == "dm:" then
    local id = providerText(trim(string.sub(raw, 4)))
    if not id then
      error("lark: empty DM target id", 0)
    end
    local isOpenId = string.sub(id, 1, 3) == "ou_"
    return {
      kind = "dm",
      receiveId = id,
      receiveIdType = isOpenId and "open_id" or "chat_id",
      chatId = id,
      openId = isOpenId and id or "",
      threadId = "",
    }
  end
  if string.sub(raw, 1, 6) == "group:" then
    local rest = string.sub(raw, 7)
    local idx = string.find(rest, ":thread:", 1, true)
    if idx then
      local chatId = providerText(string.sub(rest, 1, idx - 1))
      local threadId = providerText(string.sub(rest, idx + 8))
      if not chatId or not threadId then
        error('lark: invalid thread target "' .. raw .. '"', 0)
      end
      return {
        kind = "thread",
        receiveId = chatId,
        receiveIdType = "chat_id",
        chatId = chatId,
        openId = "",
        threadId = threadId,
      }
    end
    local chatId = providerText(rest)
    if not chatId then
      error("lark: empty group target id", 0)
    end
    return {
      kind = "group",
      receiveId = chatId,
      receiveIdType = "chat_id",
      chatId = chatId,
      openId = "",
      threadId = "",
    }
  end
  if string.sub(raw, 1, 3) == "ou_" then
    return {
      kind = "dm",
      receiveId = raw,
      receiveIdType = "open_id",
      chatId = raw,
      openId = raw,
      threadId = "",
    }
  end
  return {
    kind = "group",
    receiveId = raw,
    receiveIdType = "chat_id",
    chatId = raw,
    openId = "",
    threadId = "",
  }
end

local function conversationKey(target)
  local parsed = parseTarget(target)
  local bid = (botIdentity and botIdentity.id) or parsed.botId or ""
  if bid ~= "" then
    local chatOrOpen = parsed.kind == "dm" and parsed.receiveId or parsed.chatId
    local tid = (parsed.kind == "thread" and parsed.threadId ~= "") and parsed.threadId or "0"
    return "lark:bot:" .. bid .. ":chat:" .. chatOrOpen .. ":thread:" .. tid
  end
  if parsed.kind == "dm" then
    return "lark:" .. regionKey .. ":dm:" .. parsed.receiveId
  end
  if parsed.kind == "thread" then
    return "lark:" .. regionKey .. ":group:" .. parsed.chatId .. ":thread:" .. parsed.threadId
  end
  return "lark:" .. regionKey .. ":group:" .. parsed.chatId
end

local function groupConversationKey(chatId)
  local bid = (botIdentity and botIdentity.id) or ""
  if bid ~= "" then
    return "lark:bot:" .. bid .. ":chat:" .. chatId .. ":thread:0"
  end
  return "lark:" .. regionKey .. ":group:" .. chatId
end

-- ---------------- HTTP, Auth, Singleflight & Timeout ----------------

local function parseRetryAfterMs(headerVal)
  if not headerVal or headerVal == "" then
    return 0
  end
  local trimmed = trim(headerVal)
  local secs = tonumber(trimmed)
  if isFiniteNumber(secs) and secs > 0 then
    return math.min(math.ceil(secs * 1000), 60000)
  end
  local dateMs = parseHttpDateMs(trimmed)
  if dateMs then
    local delta = dateMs - wallNowMs()
    if delta > 0 then
      return math.min(delta, 60000)
    end
  end
  return 0
end

local function requestJson(methodName, httpMethod, path, opts)
  opts = type(opts) == "table" and opts or {}
  local params = opts.params
  local body = opts.body
  local headers = opts.headers
  local timeoutMs = opts.timeoutMs or API_TIMEOUT_MS

  local url = apiBase .. path
  if type(params) == "table" then
    local qParts = {}
    local pKeys = {}
    for k, v in pairs(params) do
      if v ~= nil and v ~= "" then
        pKeys[#pKeys + 1] = tostring(k)
      end
    end
    table.sort(pKeys)
    for _, k in ipairs(pKeys) do
      qParts[#qParts + 1] = encodeComponentStrict(k) .. "=" .. encodeComponentStrict(params[k])
    end
    if #qParts > 0 then
      local sep = string.find(url, "?", 1, true) and "&" or "?"
      url = url .. sep .. table.concat(qParts, "&")
    end
  end

  local reqHeaders = {}
  if type(headers) == "table" then
    for k, v in pairs(headers) do
      reqHeaders[k] = v
    end
  end
  local reqBody = nil
  if body ~= nil then
    reqHeaders["Content-Type"] = "application/json"
    reqBody = json.encode(body)
  end

  local req = {
    url = url,
    method = httpMethod,
    headers = reqHeaders,
    timeoutMs = clampDelayMs(timeoutMs),
  }
  if reqBody ~= nil then
    req.body = reqBody
  end

  local ok, res = pcall(http.request, req)
  if not ok then
    local errStr = scrubSecrets(errMessage(res))
    local lower = string.lower(errStr)
    if string.find(lower, "deadline exceeded", 1, true) or string.find(lower, "timeout", 1, true) then
      error(newLarkTransportError(methodName, "TimeoutError"), 0)
    end
    error(newLarkTransportError(methodName, "NetworkError", errStr), 0)
  end

  local retryAfterMs = parseRetryAfterMs(type(res.headers) == "table" and res.headers["retry-after"] or nil)
  local rawText = type(res.body) == "string" and res.body or ""

  local parsed = nil
  if rawText ~= "" then
    local decOk, dec = pcall(json.decode, rawText)
    if not decOk then
      error(
        newLarkApiError(methodName, {
          httpStatus = res.status,
          providerCode = nil,
          message = scrubSecrets("provider returned invalid JSON: " .. string.sub(rawText, 1, 120)),
          retryAfterMs = retryAfterMs,
        }),
        0
      )
    end
    parsed = dec
  else
    error(
      newLarkApiError(methodName, {
        httpStatus = res.status,
        providerCode = nil,
        message = "provider returned empty response",
        retryAfterMs = retryAfterMs,
      }),
      0
    )
  end

  if not isRecord(parsed) then
    error(
      newLarkApiError(methodName, {
        httpStatus = res.status,
        providerCode = nil,
        message = "provider response is not an object",
        retryAfterMs = retryAfterMs,
      }),
      0
    )
  end

  local providerCode = isIntegerNumber(parsed.code) and (math.tointeger(parsed.code) or math.floor(parsed.code)) or nil
  local providerMsg = scrubSecrets(
    (type(parsed.msg) == "string" and parsed.msg ~= "") and parsed.msg or "provider rejected request"
  )

  if not res.ok then
    error(
      newLarkApiError(methodName, {
        httpStatus = res.status,
        providerCode = providerCode,
        message = providerMsg,
        retryAfterMs = retryAfterMs,
      }),
      0
    )
  end
  if providerCode ~= 0 then
    error(
      newLarkApiError(methodName, {
        httpStatus = res.status,
        providerCode = providerCode,
        message = providerMsg,
        retryAfterMs = retryAfterMs,
      }),
      0
    )
  end
  return parsed
end

local function invalidateTenantToken(badToken)
  if not badToken or badToken == "" or cachedToken == badToken then
    cachedToken = ""
    tokenRefreshAt = 0
  end
end

local function refreshTenantToken()
  local issuedMs = monoNowMs()
  local body = requestJson(
    "tenant_access_token",
    "POST",
    "/open-apis/auth/v3/tenant_access_token/internal",
    {
      body = { app_id = appId, app_secret = appSecret },
      timeoutMs = API_TIMEOUT_MS,
    }
  )
  local token = type(body.tenant_access_token) == "string" and trim(body.tenant_access_token) or ""
  local expire = body.expire
  if token == "" then
    error(
      newLarkApiError("tenant_access_token", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider response is missing tenant_access_token",
      }),
      0
    )
  end
  if not isIntegerNumber(expire) or expire <= 0 then
    error(
      newLarkApiError("tenant_access_token", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider response contains an invalid expiry",
      }),
      0
    )
  end
  local marginMs = math.min(TOKEN_REFRESH_MARGIN_MS, math.max(1000, expire * 500))
  cachedToken = token
  registerSecret(token)
  tokenRefreshAt = issuedMs + expire * 1000 - marginMs
  return token
end

local function getTenantToken(forceRefresh)
  if not forceRefresh and cachedToken ~= "" and monoNowMs() < tokenRefreshAt then
    return cachedToken
  end
  local tok = tokenRefreshToken
  if tok ~= nil then
    local ok, res = task.multi_wait(tok)
    if not ok then
      error(res, 0)
    end
    return res
  end
  tok = {}
  tokenRefreshToken = tok
  local ok, res = pcall(refreshTenantToken)
  if tokenRefreshToken == tok then
    tokenRefreshToken = nil
  end
  task.multi_wakeup(tok, ok, res)
  if not ok then
    error(res, 0)
  end
  return res
end

local function larkRequest(methodName, httpMethod, path, opts)
  opts = type(opts) == "table" and opts or {}
  local params = opts.params
  local body = opts.body
  local timeoutMs = opts.timeoutMs or API_TIMEOUT_MS
  local maxRateLimitRetries = opts.maxRateLimitRetries
  if maxRateLimitRetries == nil then
    maxRateLimitRetries = MAX_RATE_LIMIT_RETRIES
  end

  local tokenRetried = false
  local forceRefresh = false
  local rateLimitAttempt = 0

  while true do
    local token = getTenantToken(forceRefresh)
    forceRefresh = false
    local ok, res = pcall(requestJson, methodName, httpMethod, path, {
      params = params,
      body = body,
      headers = { Authorization = "Bearer " .. token },
      timeoutMs = timeoutMs,
    })
    if ok then
      return res
    end
    local err = res
    if isTokenInvalidError(err) and not tokenRetried then
      invalidateTenantToken(token)
      tokenRetried = true
      forceRefresh = true
    elseif isRateLimitError(err) and rateLimitAttempt < maxRateLimitRetries and not stopping then
      local delay = (err.retryAfterMs and err.retryAfterMs > 0)
          and err.retryAfterMs
        or math.min(1000 * (1 << rateLimitAttempt), 15000)
      rateLimitAttempt = rateLimitAttempt + 1
      sleepMs(delay)
    else
      error(err, 0)
    end
  end
end

local function parseClientConfig(payload, fallback)
  local base = fallback or {
    pingInterval = 120,
    reconnectCount = -1,
    reconnectInterval = 120,
    reconnectNonce = 30,
  }
  if not isRecord(payload) then
    return {
      pingInterval = base.pingInterval,
      reconnectCount = base.reconnectCount,
      reconnectInterval = base.reconnectInterval,
      reconnectNonce = base.reconnectNonce,
    }
  end
  local function numOr(val, def, minVal)
    if isFiniteNumber(val) and (minVal == nil or val >= minVal) then
      return val
    end
    return def
  end
  return {
    pingInterval = numOr(payload.PingInterval, base.pingInterval, 1),
    reconnectCount = numOr(payload.ReconnectCount, base.reconnectCount, -1),
    reconnectInterval = numOr(payload.ReconnectInterval, base.reconnectInterval, 1),
    reconnectNonce = numOr(payload.ReconnectNonce, base.reconnectNonce, 0),
  }
end

local function fetchWsEndpoint(timeoutMs)
  local body = requestJson("ws_endpoint", "POST", "/callback/ws/endpoint", {
    body = { AppID = appId, AppSecret = appSecret },
    timeoutMs = timeoutMs or API_TIMEOUT_MS,
  })
  local data = body.data
  if not isRecord(data) then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider response is missing endpoint data",
      }),
      0
    )
  end
  local rawUrl = data.URL
  if type(rawUrl) ~= "string" or rawUrl == "" then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider response is missing endpoint URL",
      }),
      0
    )
  end
  local ok, parsedUrl = pcall(urlMod.parse, rawUrl)
  if not ok or type(parsedUrl) ~= "table" then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider endpoint URL is invalid",
      }),
      0
    )
  end
  if string.lower(parsedUrl.scheme or "") ~= "wss" or (parsedUrl.hostname or "") == "" then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider endpoint URL must use wss://",
      }),
      0
    )
  end
  local serviceIdValues = getAllQueryParams(parsedUrl.query, "service_id")
  local deviceIdValues = getAllQueryParams(parsedUrl.query, "device_id")
  if #serviceIdValues ~= 1 or serviceIdValues[1]:match("^-?%d+$") == nil then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider endpoint is missing valid service_id",
      }),
      0
    )
  end
  if #deviceIdValues ~= 1 or deviceIdValues[1] == "" then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider endpoint is missing device_id",
      }),
      0
    )
  end
  local serviceId = math.tointeger(serviceIdValues[1])
  if serviceId == nil then
    error(
      newLarkApiError("ws_endpoint", {
        httpStatus = 200,
        providerCode = 0,
        message = "provider endpoint is missing valid service_id",
      }),
      0
    )
  end
  return {
    url = rawUrl,
    serviceId = serviceId,
    deviceId = deviceIdValues[1],
    clientConfig = parseClientConfig(data.ClientConfig),
  }
end

local function parseBotInfo(payload)
  if not isRecord(payload) then
    error("lark: bot info response must be an object", 0)
  end
  local bot = isRecord(payload.bot) and payload.bot or nil
  if not bot and isRecord(payload.data) and isRecord(payload.data.bot) then
    bot = payload.data.bot
  end
  if not bot and not isNull(payload.open_id) then
    bot = payload
  end
  if not bot then
    error("lark: bot info response is missing bot data", 0)
  end
  local openId = providerText(bot.open_id)
  if not openId then
    error("lark: bot info is missing valid open_id", 0)
  end
  local rawName = not isNull(bot.app_name) and bot.app_name or bot.name
  local appName = appId
  if not isNull(rawName) then
    local validName = providerText(rawName)
    if not validName then
      error("lark: bot info contains an invalid display name", 0)
    end
    appName = validName
  end
  return { id = openId, name = appName }
end

-- ---------------- Directory Lookups & Caches ----------------

local function nameExpiryMs(name)
  return monoNowMs() + (name and NAME_HIT_TTL_MS or NAME_MISS_TTL_MS)
end

local function lookupSenderName(openId)
  local cached = senderNameCache[openId]
  if cached and cached.expiresAt > monoNowMs() then
    return cached.name
  end
  local name = nil
  local aliases = {}
  local ok, res = pcall(
    larkRequest,
    "contact_user",
    "GET",
    "/open-apis/contact/v3/users/" .. encodeComponentStrict(openId),
    { params = { user_id_type = "open_id" }, timeoutMs = CALL_TIMEOUT_MS, maxRateLimitRetries = 1 }
  )
  if ok and isRecord(res) then
    local user = (isRecord(res.data) and isRecord(res.data.user)) and res.data.user
      or (isRecord(res.user) and res.user)
      or nil
    if user then
      for _, field in ipairs({ "name", "en_name", "nickname" }) do
        local candidate = providerText(user[field])
        if candidate then
          if not name then
            name = candidate
          end
          aliases[#aliases + 1] = candidate
        end
      end
    end
  elseif not ok then
    log.warn("lark contact user lookup failed", {
      openId = openId,
      error = scrubSecrets(errMessage(res)),
    })
  end
  senderNameCache[openId] = { expiresAt = nameExpiryMs(name), name = name }
  for _, alias in ipairs(aliases) do
    kvSetAsync(KV_DM_HANDLE_PREFIX .. string.lower(alias), openId)
  end
  return name
end

local function lookupGroupName(chatId)
  local cached = groupNameCache[chatId]
  if cached and cached.expiresAt > monoNowMs() then
    return cached.name
  end
  local name = nil
  local ok, res = pcall(
    larkRequest,
    "chat_get",
    "GET",
    "/open-apis/im/v1/chats/" .. encodeComponentStrict(chatId),
    { timeoutMs = CALL_TIMEOUT_MS, maxRateLimitRetries = 1 }
  )
  if ok and isRecord(res) then
    local data = isRecord(res.data) and res.data or res
    name = providerText(data.name)
  elseif not ok then
    log.warn("lark chat title lookup failed", {
      chatId = chatId,
      error = scrubSecrets(errMessage(res)),
    })
  end
  groupNameCache[chatId] = { expiresAt = nameExpiryMs(name), name = name }
  return name
end

local function rememberDmChat(openId, chatId)
  if not openId or openId == "" or not chatId or chatId == "" then
    return
  end
  dmChatIds[openId] = chatId
  kvSet(KV_DM_CHAT_PREFIX .. openId, chatId)
end

local function getDmChatId(openId)
  if not openId or openId == "" then
    return ""
  end
  local inMem = dmChatIds[openId]
  if inMem then
    return inMem
  end
  local fromKv = kvGet(KV_DM_CHAT_PREFIX .. openId)
  local valid = providerText(fromKv)
  if valid then
    dmChatIds[openId] = valid
    return valid
  end
  return ""
end

local function rememberTopicRoot(threadId, rootMessageId)
  if not threadId or threadId == "" or not rootMessageId or rootMessageId == "" then
    return
  end
  topicRoots[threadId] = rootMessageId
  kvSet(KV_TOPIC_ROOT_PREFIX .. threadId, rootMessageId)
end

local function getTopicRoot(threadId)
  if not threadId or threadId == "" then
    return nil
  end
  local inMem = topicRoots[threadId]
  if inMem then
    return inMem
  end
  local fromKv = kvGet(KV_TOPIC_ROOT_PREFIX .. threadId)
  local valid = providerText(fromKv)
  if valid then
    topicRoots[threadId] = valid
    return valid
  end
  return nil
end

-- ---------------- Inbound Attachments & Message Mapping ----------------

local function downloadResourceAttachment(messageId, resource)
  if not resource.fileKey then
    return {
      filename = resource.name,
      contentType = resource.mediaType or "",
      status = "failed",
      errorReason = "missing_resource_key",
    }
  end
  local downloadType = DOWNLOAD_TYPES[resource.resourceType]
  if not downloadType then
    return {
      filename = resource.name,
      contentType = resource.mediaType or "",
      status = "failed",
      errorReason = "resource_download_unsupported",
    }
  end
  if not attachmentsMod then
    return {
      filename = resource.name,
      contentType = resource.mediaType or "",
      status = "failed",
      errorReason = "attachments_permission_unavailable",
    }
  end

  local url = apiBase
    .. "/open-apis/im/v1/messages/"
    .. encodeComponentStrict(messageId)
    .. "/resources/"
    .. encodeComponentStrict(resource.fileKey)
    .. "?type="
    .. encodeComponentStrict(downloadType)
  local fallbackCt = fallbackContentType(resource)

  local tokenRetried = false
  local forceRefresh = false
  local rateLimitAttempt = 0

  while true do
    local tokOk, token = pcall(getTenantToken, forceRefresh)
    forceRefresh = false
    if not tokOk then
      local reason = string.sub(scrubSecrets(errMessage(token)), 1, 256)
      log.warn("lark attachment token failed", { messageId = messageId, error = reason })
      return {
        filename = resource.name,
        contentType = fallbackCt,
        status = "failed",
        errorReason = reason,
      }
    end

    local dlReq = {
      url = url,
      headers = { Authorization = "Bearer " .. token },
      filename = resource.explicitName or "",
      timeoutMs = MEDIA_TIMEOUT_MS,
    }
    if fallbackCt ~= "" then
      dlReq.contentType = fallbackCt
    end

    local dlOk, meta = pcall(attachmentsMod.download, dlReq)
    if dlOk then
      local responseName = (meta.filename and meta.filename ~= "" and meta.filename ~= meta.attachmentId)
          and meta.filename
        or nil
      local resolvedCt = (meta.contentType and meta.contentType ~= "") and meta.contentType or fallbackCt
      local finalName = resourceName({
        eventName = resource.name,
        responseName = responseName,
        mediaType = resolvedCt ~= "" and resolvedCt or nil,
        resourceType = resource.resourceType,
      })
      return {
        attachmentId = meta.attachmentId,
        filename = finalName,
        contentType = resolvedCt,
        sizeBytes = meta.size,
        sha256 = meta.sha256,
      }
    end

    local errMsg = scrubSecrets(errMessage(meta))
    if not tokenRetried and errMsg:find("%f[%w]HTTP 401%f[%W]") ~= nil then
      invalidateTenantToken(token)
      tokenRetried = true
      forceRefresh = true
    elseif rateLimitAttempt < 2 and errMsg:find("%f[%w]HTTP 429%f[%W]") ~= nil and not stopping then
      rateLimitAttempt = rateLimitAttempt + 1
      sleepMs(1000 * rateLimitAttempt)
    else
      log.warn("lark attachment download failed", {
        messageId = messageId,
        resourceType = resource.resourceType,
        error = errMsg,
      })
      return {
        filename = resource.name,
        contentType = fallbackCt,
        status = "failed",
        errorReason = string.sub(errMsg, 1, 256),
      }
    end
  end
end

local function readSenderOpenId(sender)
  if not isRecord(sender) then
    return nil
  end
  return isRecord(sender.sender_id) and providerText(sender.sender_id.open_id) or nil
end

local function optionalReference(value)
  if isNull(value) or value == "" then
    return nil, true
  end
  local text = providerText(value)
  return text, text ~= nil
end

local function resolveSenderInfo(sender)
  local openId = readSenderOpenId(sender)
  if not openId or not isRecord(sender) then
    return {
      senderId = "",
      senderHandle = "unknown",
      senderName = "unknown",
      senderType = "unknown",
    }
  end
  local rawType = providerText(sender.sender_type)
  if rawType and AGENT_SENDER_TYPES[rawType] then
    local isSelf = botIdentity and openId == botIdentity.id
    local label = isSelf and botIdentity.name or openId
    return {
      senderId = openId,
      senderHandle = label,
      senderName = label,
      senderType = "agent",
    }
  end
  if rawType ~= "user" then
    return {
      senderId = openId,
      senderHandle = openId,
      senderName = openId,
      senderType = "unknown",
    }
  end
  local displayName = lookupSenderName(openId)
  return {
    senderId = openId,
    senderHandle = openId,
    senderName = displayName or "",
    senderType = "human",
  }
end

local function adaptParentMessage(raw)
  local parent = {}
  for k, v in pairs(raw) do
    parent[k] = v
  end
  if not providerText(parent.message_type) then
    local msgType = providerText(parent.msg_type)
    if msgType then
      parent.message_type = msgType
    end
  end
  if isNull(parent.content) and isRecord(parent.body) and not isNull(parent.body.content) then
    parent.content = parent.body.content
  end
  local sender = parent.sender
  if isRecord(sender) and not isRecord(sender.sender_id) then
    local sid = providerText(sender.id)
    local idType = providerText(sender.id_type) or "open_id"
    if sid and idType == "open_id" then
      local copiedSender = {}
      for k, v in pairs(sender) do
        copiedSender[k] = v
      end
      copiedSender.sender_id = { open_id = sid }
      parent.sender = copiedSender
    end
  end
  return parent
end

local function buildInboundEvent(msg, opts)
  local messageId = providerText(msg.message_id)
  local messageType = providerText(msg.message_type)
  if not messageId or not messageType then
    error("lark: message is missing provider identity", 0)
  end
  local mentions = parseMentions(msg.mentions)
  local projected = projectContent(messageType, msg.content, {
    mentions = mentions,
    botOpenId = (botIdentity and botIdentity.id) or "",
  })

  local attachments = {}
  for _, res in ipairs(projected.resources) do
    attachments[#attachments + 1] = downloadResourceAttachment(messageId, res)
  end

  local senderInfo = resolveSenderInfo(opts.sender)
  local resolvedDisplayName = (opts.displayName and opts.displayName ~= "")
      and opts.displayName
    or (
      opts.kind == "dm"
        and ((senderInfo.senderName ~= "" and senderInfo.senderName) or senderInfo.senderId)
      or opts.target
    )

  local mentionsAgent = false
  if opts.forceMentionsAgent ~= nil then
    mentionsAgent = opts.forceMentionsAgent and true or false
  elseif opts.kind == "dm" then
    mentionsAgent = true
  else
    mentionsAgent = (projected.mentionedBot and true or false) or (opts.replyToBot == true)
    if not mentionsAgent and botIdentity then
      for _, m in pairs(mentions) do
        if m.openId == botIdentity.id or ((not m.openId or m.openId == "") and m.displayName == botIdentity.name) then
          mentionsAgent = true
          break
        end
      end
    end
  end

  local occurredAtMs = providerTimeMs(msg.create_time)
  local ev = {
    type = "message",
    target = opts.target,
    conversationKey = opts.convKey,
    kind = opts.kind,
    displayName = resolvedDisplayName,
    providerMsgId = messageId,
    senderId = senderInfo.senderId,
    senderHandle = senderInfo.senderHandle,
    senderName = senderInfo.senderName,
    senderType = senderInfo.senderType,
    body = projected.body,
    replyToId = opts.replyToId or "",
    mentionsAgent = mentionsAgent,
    silent = opts.silent and true or false,
    timestampMs = occurredAtMs ~= nil and occurredAtMs or wallNowMs(),
    attachments = json.array(attachments),
  }
  if opts.parentConvKey and opts.parentConvKey ~= "" then
    ev.parentConversationKey = opts.parentConvKey
    if opts.parentTarget and opts.parentTarget ~= "" then
      ev.parentTarget = opts.parentTarget
    end
    if opts.parentDisplayName and opts.parentDisplayName ~= "" then
      ev.parentDisplayName = opts.parentDisplayName
    end
  end
  return ev
end

local function fetchQuotedParent(parentId, ctx)
  local ok, res = pcall(
    larkRequest,
    "message_get",
    "GET",
    "/open-apis/im/v1/messages/" .. encodeComponentStrict(parentId),
    { params = { user_id_type = "open_id" }, timeoutMs = CALL_TIMEOUT_MS, maxRateLimitRetries = 1 }
  )
  if not ok then
    log.warn("lark quoted parent fetch failed", {
      parentId = parentId,
      error = scrubSecrets(errMessage(res)),
    })
    return nil
  end
  local items = isRecord(res.data) and res.data.items or nil
  if not isArray(items) or #items == 0 or not isRecord(items[1]) then
    log.warn("lark quoted parent malformed response", { parentId = parentId })
    return nil
  end
  local parent = adaptParentMessage(items[1])
  if not providerText(parent.message_id) then
    log.warn("lark quoted parent missing message_id", { parentId = parentId })
    return nil
  end
  if not isNull(parent.chat_id) and providerText(parent.chat_id) ~= ctx.chatId then
    log.warn("lark quoted parent chat_id mismatch", { parentId = parentId, chatId = ctx.chatId })
    return nil
  end
  local buildOk, ev = pcall(buildInboundEvent, parent, {
    target = ctx.target,
    convKey = ctx.convKey,
    parentConvKey = ctx.parentConvKey,
    parentTarget = ctx.parentTarget,
    parentDisplayName = ctx.parentDisplayName,
    kind = ctx.kind,
    displayName = ctx.displayName,
    sender = parent.sender,
    replyToId = "",
    forceMentionsAgent = false,
    silent = true,
  })
  if not buildOk then
    log.warn("lark quoted parent fetch failed", {
      parentId = parentId,
      error = scrubSecrets(errMessage(ev)),
    })
    return nil
  end
  return ev
end

local function processInboundEvent(payloadObj)
  if not emitFn or not isRecord(payloadObj) or not botIdentity then
    return
  end
  if payloadObj.schema ~= "2.0" then
    return
  end
  local header = payloadObj.header
  local event = payloadObj.event
  if not isRecord(header) or not isRecord(event) then
    return
  end
  if header.event_type ~= "im.message.receive_v1" then
    return
  end

  local msg = event.message
  local sender = event.sender
  if not isRecord(msg) or not isRecord(sender) then
    return
  end

  local messageId = providerText(msg.message_id)
  local chatId = providerText(msg.chat_id)
  local chatType = providerText(msg.chat_type)
  local messageType = providerText(msg.message_type)
  local senderOpenId = readSenderOpenId(sender)

  if not messageId or not chatId or not messageType or not senderOpenId then
    return
  end
  if not chatType or not VALID_CHAT_TYPES[chatType] then
    return
  end

  -- Self-filter ANY message sent by the bot itself regardless of sender_type.
  if senderOpenId == botIdentity.id then
    return
  end

  local threadVal, threadOk = optionalReference(msg.thread_id)
  if not threadOk then
    return
  end
  local parentVal, parentOk = optionalReference(msg.parent_id)
  if not parentOk or parentVal == messageId then
    return
  end
  local rootVal, rootOk = optionalReference(msg.root_id)
  if not rootOk then
    return
  end

  local isDM = chatType == "p2p"
  local threadId = (not isDM) and threadVal or nil
  local parentId = parentVal
  local rootId = rootVal

  -- Unify DM targets strictly on peer open_id (dm:${senderOpenId}) while mapping oc_ chat_id.
  local target
  if isDM then
    target = "dm:" .. senderOpenId
  elseif threadId then
    target = "group:" .. chatId .. ":thread:" .. threadId
  else
    target = "group:" .. chatId
  end
  local kind = isDM and "dm" or (threadId and "thread" or "group")
  local convKey = conversationKey(target)
  local parentConvKey = (not isDM and threadId) and groupConversationKey(chatId) or ""

  if isDM then
    rememberDmChat(senderOpenId, chatId)
  end
  if threadId then
    rememberTopicRoot(threadId, rootId or messageId)
  end

  local displayName
  if isDM then
    local senderInfo = resolveSenderInfo(sender)
    displayName = (senderInfo.senderName ~= "" and senderInfo.senderName) or senderOpenId
  else
    local groupTitle = lookupGroupName(chatId)
    displayName = groupTitle or chatId
  end

  local parentTarget = (not isDM and threadId) and ("group:" .. chatId) or ""
  local replyToBot = false
  if parentId then
    local quotedEvent = fetchQuotedParent(parentId, {
      chatId = chatId,
      target = target,
      convKey = convKey,
      parentConvKey = parentConvKey,
      parentTarget = parentTarget,
      parentDisplayName = displayName,
      kind = kind,
      displayName = displayName,
    })
    if quotedEvent then
      if botIdentity and quotedEvent.senderId == botIdentity.id then
        replyToBot = true
      end
      if emitFn and not stopping then
        emitFn(quotedEvent)
      end
    end
  end

  local childEvent = buildInboundEvent(msg, {
    target = target,
    convKey = convKey,
    parentConvKey = parentConvKey,
    parentTarget = parentTarget,
    parentDisplayName = displayName,
    kind = kind,
    displayName = displayName,
    sender = sender,
    replyToId = parentId or "",
    replyToBot = replyToBot,
    forceMentionsAgent = nil,
    silent = false,
  })

  -- 0.3 parity: preserve bare @BotName pings even when stripped body is empty.
  if childEvent.body == "" and #childEvent.attachments == 0 and not childEvent.mentionsAgent then
    return
  end
  if emitFn and not stopping then
    emitFn(childEvent)
  end
end

local function startEventWorker()
  if eventChannel then
    eventChannel:close()
  end
  local ch = task.channel()
  eventChannel = ch
  task.fork(function()
    for payloadObj in ch:iter() do
      if stopping or eventChannel ~= ch then
        break
      end
      local ok, err = pcall(processInboundEvent, payloadObj)
      if not ok then
        log.warn("lark event processing failed", {
          error = scrubSecrets(errMessage(err)),
        })
      end
    end
  end)
  return ch
end

local function enqueueInboundEvent(payloadObj)
  local ch = eventChannel
  if ch and not ch:closed() then
    ch:try_send(payloadObj)
  end
end

local function resetDedup()
  dedupNodes = {}
  dedupHead = nil
  dedupTail = nil
  dedupSize = 0
end

local function isDuplicateEventId(eventId)
  if not eventId or eventId == "" then
    return false
  end
  local existing = dedupNodes[eventId]
  if existing then
    -- Move to tail (most recently seen)
    if existing ~= dedupTail then
      if existing.prev then
        existing.prev.next = existing.next
      else
        dedupHead = existing.next
      end
      if existing.next then
        existing.next.prev = existing.prev
      end
      existing.prev = dedupTail
      existing.next = nil
      if dedupTail then
        dedupTail.next = existing
      else
        dedupHead = existing
      end
      dedupTail = existing
    end
    return true
  end

  local node = { key = eventId, prev = dedupTail, next = nil }
  dedupNodes[eventId] = node
  if dedupTail then
    dedupTail.next = node
  else
    dedupHead = node
  end
  dedupTail = node
  dedupSize = dedupSize + 1

  while dedupSize > MAX_DEDUP_EVENTS and dedupHead do
    local oldest = dedupHead
    dedupHead = oldest.next
    if dedupHead then
      dedupHead.prev = nil
    else
      dedupTail = nil
    end
    oldest.next = nil
    oldest.prev = nil
    dedupNodes[oldest.key] = nil
    dedupSize = dedupSize - 1
  end
  return false
end

-- ---------------- Interactive Approval Cards (Card JSON 2.0) ----------------

local function buildApprovalCard(entry, statusText)
  local bodyText = trim(entry.body)
  local cpLen = utf8.len(bodyText)
  if cpLen and cpLen > 4000 then
    local cut = utf8.offset(bodyText, 4000)
    if cut and cut > 1 then
      bodyText = string.sub(bodyText, 1, cut - 1) .. "…"
    end
  elseif not cpLen and #bodyText > 4000 then
    bodyText = string.sub(bodyText, 1, 3999) .. "…"
  end

  local elements = {}
  if bodyText ~= "" then
    elements[#elements + 1] = { tag = "markdown", content = bodyText }
  end
  if statusText == nil then
    local columns = {}
    for _, a in ipairs(entry.actions) do
      local btnType = "default"
      if a.style == "primary" then
        btnType = "primary"
      elseif a.style == "danger" then
        btnType = "danger"
      end
      columns[#columns + 1] = {
        tag = "column",
        elements = json.array({
          {
            tag = "button",
            type = btnType,
            text = { tag = "plain_text", content = tostring(a.label ~= "" and a.label or a.id) },
            behaviors = json.array({
              {
                type = "callback",
                value = {
                  action = tostring(a.id),
                  token = entry.token,
                },
              },
            }),
          },
        }),
      }
    end
    if #columns > 0 then
      elements[#elements + 1] = {
        tag = "column_set",
        columns = json.array(columns),
      }
    end
  else
    elements[#elements + 1] = { tag = "markdown", content = tostring(statusText) }
  end

  local titleText = (entry.title ~= nil and entry.title ~= "") and tostring(entry.title) or "Approval required"
  return {
    schema = "2.0",
    config = { update_multi = true },
    header = {
      template = "blue",
      title = {
        tag = "plain_text",
        content = titleText,
      },
    },
    body = { elements = json.array(elements) },
  }
end

local function buildToastAck(text, kind)
  local toastJson = json.encode({
    toast = { type = kind or "info", content = text },
  })
  local encodedToast = crypto.base64Encode(toastJson)
  return json.encode({ code = 200, data = encodedToast })
end

local function startTrackedCardTask(fn)
  local tracker = { done = false, token = {} }
  activeCardTasks[tracker] = true
  task.fork(function()
    local ok, err = pcall(fn)
    tracker.done = true
    activeCardTasks[tracker] = nil
    task.multi_wakeup(tracker.token)
    if not ok then
      log.warn("lark card task failed", { error = scrubSecrets(errMessage(err)) })
    end
  end)
  return tracker
end

local function awaitTrackedTask(tracker)
  if not tracker or tracker.done then
    return
  end
  pcall(task.multi_wait, tracker.token)
end

local function redrawCard(updateToken, card)
  local ok, err = pcall(larkRequest, "card_update", "POST", "/open-apis/interactive/v1/card/update", {
    body = { token = updateToken, card = card },
    timeoutMs = CARD_UPDATE_TIMEOUT_MS,
    maxRateLimitRetries = 1,
  })
  if not ok then
    log.warn("lark card update failed", { error = scrubSecrets(errMessage(err)) })
  end
end

local function patchCardMessage(messageId, card)
  local ok, err = pcall(
    larkRequest,
    "message_patch",
    "PATCH",
    "/open-apis/im/v1/messages/" .. encodeComponentStrict(messageId),
    {
      body = { content = json.encode(card) },
      timeoutMs = CARD_UPDATE_TIMEOUT_MS,
      maxRateLimitRetries = 1,
    }
  )
  if not ok then
    log.warn("lark card close patch failed", {
      messageId = messageId,
      error = scrubSecrets(errMessage(err)),
    })
  end
end

local function closePendingPrompt(entry, statusText)
  if not entry or entry.settled then
    return
  end
  entry.settled = true
  cancelTimer(entry.timer)
  entry.timer = nil
  pendingByToken[entry.token] = nil
  pendingByPromptId[entry.promptId] = nil
  if not entry.messageId then
    return
  end
  local card = buildApprovalCard(entry, statusText)
  local tracker = startTrackedCardTask(function()
    patchCardMessage(entry.messageId, card)
  end)
  awaitTrackedTask(tracker)
end

local function parseCardCallback(payloadObj)
  if not isRecord(payloadObj) then
    return nil
  end
  local event = payloadObj.event
  if not isRecord(event) then
    return nil
  end
  local operator = event.operator
  local context = event.context
  local action = event.action
  if not isRecord(operator) or not isRecord(context) or not isRecord(action) then
    return nil
  end
  if action.tag ~= "button" then
    return nil
  end
  local value = action.value
  if not isRecord(value) then
    return nil
  end

  local operatorId = providerText(operator.open_id)
  local chatId = providerText(context.open_chat_id)
  local messageId = providerText(context.open_message_id)
  local actionId = providerText(value.action)
  local token = providerText(value.token)
  local updateToken = providerText(event.token)

  if not (operatorId and chatId and messageId and actionId and token and updateToken) then
    return nil
  end
  return {
    operatorId = operatorId,
    chatId = chatId,
    messageId = messageId,
    actionId = actionId,
    token = token,
    updateToken = updateToken,
  }
end

local function handleCardCallback(payloadObj)
  local cb = parseCardCallback(payloadObj)
  if not cb then
    log.warn("lark card callback rejected", { reason = "invalid_callback" })
    return { payload = buildToastAck("Approval is no longer valid", "warning"), after = nil }
  end

  local entry = pendingByToken[cb.token]
  if not entry or entry.settled then
    log.warn("lark card callback rejected", { reason = "not_pending" })
    return { payload = buildToastAck("Approval is no longer valid", "warning"), after = nil }
  end

  -- Fail-closed actor verification.
  if not entry.allowedActorId or entry.allowedActorId == "" or cb.operatorId ~= entry.allowedActorId then
    log.warn("lark card callback rejected", { reason = "sender_mismatch" })
    return {
      payload = buildToastAck("This approval belongs to another user", "warning"),
      after = nil,
    }
  end

  if cb.chatId ~= entry.chatId or (entry.messageId ~= nil and cb.messageId ~= entry.messageId) then
    log.warn("lark card callback rejected", { reason = "route_mismatch" })
    return { payload = buildToastAck("Approval is no longer valid", "warning"), after = nil }
  end

  if not entry.actionIds[cb.actionId] then
    log.warn("lark card callback rejected", { reason = "unknown_action" })
    return { payload = buildToastAck("Unknown approval action", "warning"), after = nil }
  end

  entry.settled = true
  entry.messageId = cb.messageId
  cancelTimer(entry.timer)
  entry.timer = nil
  pendingByToken[entry.token] = nil
  pendingByPromptId[entry.promptId] = nil

  if emitFn then
    emitFn({
      type = "interaction_response",
      promptId = entry.promptId,
      actionId = cb.actionId,
      responderId = cb.operatorId,
      target = entry.target,
      timestampMs = wallNowMs(),
    })
  end

  local actionLabel = entry.actionLabels[cb.actionId] or cb.actionId
  local statusLabel = actionLabel
  if cb.actionId == "deny" or cb.actionId == "reject" then
    statusLabel = "Rejected"
  elseif cb.actionId == "approve" then
    statusLabel = "Approved"
  end
  local decidedCard = buildApprovalCard(entry, statusLabel)

  return {
    payload = buildToastAck(statusLabel, "success"),
    after = function()
      local tracker = startTrackedCardTask(function()
        redrawCard(cb.updateToken, decidedCard)
      end)
      awaitTrackedTask(tracker)
    end,
  }
end

-- ---------------- WebSocket Framing & Transport ----------------

local function requiredHeader(frame, key)
  local values = getHeaderValues(frame, key)
  if #values ~= 1 or values[1] == "" then
    error(newFrameDecodeError("lark frame: missing unique " .. key .. " header"), 0)
  end
  return values[1]
end

local function integerHeader(frame, key)
  local raw = requiredHeader(frame, key)
  if raw:match("^-?%d+$") == nil then
    error(newFrameDecodeError("lark frame: " .. key .. " header is not an integer"), 0)
  end
  local n = math.tointeger(raw)
  if n == nil then
    error(newFrameDecodeError("lark frame: " .. key .. " header is not an integer"), 0)
  end
  return n
end

local function assembleFramePayload(frame)
  local total = integerHeader(frame, HEADER_SUM)
  local seq = integerHeader(frame, HEADER_SEQ)
  if total < 1 or seq < 0 or seq >= total then
    error(newFrameDecodeError("lark frame: fragment sequence outside bounds"), 0)
  end
  local payload = frame.payload or ""
  if total == 1 then
    return payload
  end

  local currentMs = monoNowMs()
  local expired = {}
  for key, bucket in pairs(fragments) do
    if currentMs - bucket.updatedMs > FRAGMENT_EXPIRY_MS then
      expired[#expired + 1] = key
    end
  end
  for _, key in ipairs(expired) do
    fragments[key] = nil
  end

  local msgId = requiredHeader(frame, HEADER_MESSAGE_ID)
  local bucket = fragments[msgId]
  if not bucket then
    bucket = { total = total, updatedMs = currentMs, count = 0, parts = {} }
    fragments[msgId] = bucket
  elseif bucket.total ~= total then
    fragments[msgId] = nil
    error(newFrameDecodeError("lark frame: fragment count changed for message"), 0)
  end

  if bucket.parts[seq] == nil then
    bucket.count = bucket.count + 1
  end
  bucket.parts[seq] = payload
  bucket.updatedMs = currentMs
  if bucket.count < total then
    return nil
  end

  fragments[msgId] = nil
  local ordered = {}
  for i = 0, total - 1 do
    local part = bucket.parts[i]
    if part == nil then
      error(newFrameDecodeError("lark frame: incomplete fragment set"), 0)
    end
    ordered[#ordered + 1] = part
  end
  return table.concat(ordered)
end

local function sendAckFrame(ws, frame, startMs, payloadBytes)
  if not ws then
    return
  end
  local headers = {}
  for _, h in ipairs(frame.headers or {}) do
    headers[#headers + 1] = { key = h.key, value = h.value }
  end
  headers[#headers + 1] = {
    key = HEADER_BIZ_RT,
    value = tostring(math.max(0, monoNowMs() - startMs)),
  }
  local ack = encodeFrame({
    seqId = frame.seqId,
    logId = frame.logId,
    service = frame.service,
    method = frame.method,
    headers = headers,
    payloadEncoding = frame.payloadEncoding,
    payloadType = frame.payloadType,
    payload = payloadBytes,
    logIdNew = frame.logIdNew,
  })
  ws:send(ack, true)
end

local function statusAckBytes(code)
  return json.encode({ code = code })
end

local function answerCardFrame(ws, frame, startMs, payloadObj)
  local answer = handleCardCallback(payloadObj)
  local ok = pcall(sendAckFrame, ws, frame, startMs, answer.payload)
  if not ok then
    -- Reconnect loop handles closed sockets.
    return
  end
  if answer.after then
    answer.after()
  end
end

local function handleBinaryFrame(ws, raw)
  local decOk, frame = pcall(decodeFrame, raw)
  if not decOk then
    log.warn("lark frame decode error", { error = scrubSecrets(errMessage(frame)) })
    return
  end

  local hdrOk, kind = pcall(requiredHeader, frame, HEADER_TYPE)
  if not hdrOk then
    log.warn("lark frame header error", { error = scrubSecrets(errMessage(kind)) })
    return
  end

  if frame.method == CONTROL_METHOD then
    if kind == MESSAGE_PING then
      local pong = encodeFrame({
        seqId = frame.seqId,
        logId = frame.logId,
        service = frame.service,
        method = CONTROL_METHOD,
        headers = { { key = HEADER_TYPE, value = MESSAGE_PONG } },
        payload = frame.payload,
        logIdNew = frame.logIdNew,
      })
      pcall(ws.send, ws, pong, true)
    elseif kind == MESSAGE_PONG and type(frame.payload) == "string" and #frame.payload > 0 then
      local cfgOk, cfg = pcall(json.decode, frame.payload)
      if cfgOk then
        if activeEndpoint then
          activeEndpoint = {
            url = activeEndpoint.url,
            serviceId = activeEndpoint.serviceId,
            deviceId = activeEndpoint.deviceId,
            clientConfig = parseClientConfig(cfg, activeEndpoint.clientConfig),
          }
        end
      else
        log.warn("lark pong config decode error", {
          error = scrubSecrets(errMessage(cfg)),
        })
      end
    end
    return
  end

  if frame.method ~= DATA_METHOD then
    log.warn("lark unsupported frame method", { method = frame.method })
    return
  end
  if kind ~= MESSAGE_EVENT and kind ~= MESSAGE_CARD then
    log.warn("lark unsupported data frame type", { type = kind })
    return
  end

  local startMs = monoNowMs()
  local asmOk, fullBytes = pcall(assembleFramePayload, frame)
  if not asmOk then
    log.warn("lark fragment assembly error", { error = scrubSecrets(errMessage(fullBytes)) })
    return
  end
  if fullBytes == nil then
    return
  end

  local jsonOk, payloadObj = pcall(json.decode, fullBytes)
  if not jsonOk or not isRecord(payloadObj) then
    pcall(sendAckFrame, ws, frame, startMs, statusAckBytes(500))
    return
  end

  local eventType = (isRecord(payloadObj.header) and type(payloadObj.header.event_type) == "string")
      and payloadObj.header.event_type
    or nil
  if kind == MESSAGE_CARD or (eventType ~= nil and CARD_EVENT_TYPES[eventType]) then
    task.fork(answerCardFrame, ws, frame, startMs, payloadObj)
    return
  end

  -- Immediately ACK event frame before queuing work.
  pcall(sendAckFrame, ws, frame, startMs, statusAckBytes(200))

  local eventId = isRecord(payloadObj.header) and providerText(payloadObj.header.event_id) or nil
  if eventId and isDuplicateEventId(eventId) then
    return
  end
  enqueueInboundEvent(payloadObj)
end

local function currentPingIntervalMs(ep)
  local cfg = (activeEndpoint and activeEndpoint.clientConfig) or (ep and ep.clientConfig)
  local intervalSec = (cfg and cfg.pingInterval) or 120
  return math.max(1000, math.ceil(intervalSec * 1000))
end

local function sendAppPing(ws, serviceId)
  local ping = encodeFrame({
    seqId = 0,
    logId = 0,
    service = serviceId,
    method = CONTROL_METHOD,
    headers = { { key = HEADER_TYPE, value = MESSAGE_PING } },
  })
  return pcall(ws.send, ws, ping, true)
end

local function dialWebSocketOnce(timeoutMs)
  local ep = fetchWsEndpoint(timeoutMs)
  local ws = wsClient.connect(ep.url, {
    pingIntervalMs = WS_HEARTBEAT_INTERVAL_MS,
    timeoutMs = clampDelayMs(timeoutMs),
  })
  return ws, ep
end

local function runWebSocketReadLoop(gen, rawWs, ep)
  local ws <close> = rawWs
  local nextPingAt = monoNowMs() + currentPingIntervalMs(ep)

  while not stopping and wsGeneration == gen and wsHandle == ws do
    local now = monoNowMs()
    if now >= nextPingAt then
      local sendOk, sendErr = sendAppPing(ws, ep.serviceId)
      if not sendOk then
        return "error", nil, errMessage(sendErr)
      end
      nextPingAt = monoNowMs() + currentPingIntervalMs(ep)
    end

    local waitMs = clampDelayMs(math.max(100, nextPingAt - monoNowMs()))
    local recvOk, data, kind, a3, a4 = pcall(ws.recv, ws, waitMs)
    if not recvOk then
      return "error", nil, errMessage(data)
    end

    if data ~= nil then
      if kind == "binary" and type(data) == "string" then
        handleBinaryFrame(ws, data)
        -- PONG may have updated clientConfig.pingInterval to a shorter interval.
        local updatedAt = monoNowMs() + currentPingIntervalMs(ep)
        if updatedAt < nextPingAt then
          nextPingAt = updatedAt
        end
      end
    elseif kind == "timeout" then
      if stopping or wsGeneration ~= gen or wsHandle ~= ws then
        return "closed", 1000, "disconnect"
      end
      local sendOk, sendErr = sendAppPing(ws, ep.serviceId)
      if not sendOk then
        return "error", nil, errMessage(sendErr)
      end
      nextPingAt = monoNowMs() + currentPingIntervalMs(ep)
    elseif kind == "error" then
      return "error", nil, type(a3) == "string" and a3 or "websocket error"
    else
      return "closed", a3, type(a4) == "string" and a4 or ""
    end
  end

  return "closed", 1000, "disconnect"
end

local function reconnectWebSocket(gen)
  while not stopping and wsGeneration == gen do
    reconnectAttempts = reconnectAttempts + 1
    local cfg = (activeEndpoint and activeEndpoint.clientConfig) or parseClientConfig(nil)
    if cfg.reconnectCount >= 0 and reconnectAttempts > cfg.reconnectCount then
      local msg = "lark: reconnect limit reached"
      log.error(msg, { reconnectCount = cfg.reconnectCount })
      if emitFn then
        emitFn({ type = "error", error = msg, timestampMs = wallNowMs() })
      end
      return nil, nil
    end

    local cfgIntervalMs = math.max(1000, math.ceil((cfg.reconnectInterval or 120) * 1000))
    local maxBackoffMs = math.max(30000, cfgIntervalMs)
    local baseDelay = math.min(reconnectBackoffMs, cfgIntervalMs)
    local nonceCapMs = math.min(math.max(0, math.ceil((cfg.reconnectNonce or 0) * 1000)), 5000)
    local jitterMs = nonceCapMs > 0 and randomIntBelow(nonceCapMs) or 0
    local delayMs = baseDelay + jitterMs
    reconnectBackoffMs = math.min(reconnectBackoffMs * 2, maxBackoffMs)

    local waitOk = false
    do
      local timer <close> = timers.create(clampDelayMs(delayMs))
      reconnectTimer = timer
      waitOk = pcall(timer.wait, timer)
      if reconnectTimer == timer then
        reconnectTimer = nil
      end
    end
    if not waitOk or stopping or wsGeneration ~= gen then
      return nil, nil
    end

    local openOk, ws, ep = pcall(dialWebSocketOnce, API_TIMEOUT_MS)
    if openOk then
      if stopping or wsGeneration ~= gen then
        pcall(ws.close, ws, 1000, "disconnect")
        return nil, nil
      end
      activeEndpoint = ep
      wsHandle = ws
      reconnectAttempts = 0
      reconnectBackoffMs = 1000
      if emitFn then
        emitFn({ type = "connected", identity = botIdentity, timestampMs = wallNowMs() })
      end
      log.info("lark ws reconnected", { region = regionKey })
      return ws, ep
    end

    local err = ws
    local safeMsg = scrubSecrets(errMessage(err))
    log.warn("lark reconnect failed", {
      attempt = reconnectAttempts,
      error = safeMsg,
    })
    if emitFn then
      emitFn({ type = "error", error = safeMsg, timestampMs = wallNowMs() })
    end
    if isFatalAuthError(err) then
      log.error("lark fatal auth error; stopping reconnect loop", { error = safeMsg })
      return nil, nil
    end
  end
  return nil, nil
end

local function runWebSocketSupervisor(gen, initialWs, initialEp)
  local currentWs = initialWs
  local currentEp = initialEp

  while currentWs ~= nil do
    local closeKind, closeCode, closeReason = runWebSocketReadLoop(gen, currentWs, currentEp)
    currentWs = nil

    if wsGeneration == gen then
      wsHandle = nil
    end
    fragments = {}

    if stopping or wsGeneration ~= gen then
      return
    end

    if closeKind == "error" then
      local safe = scrubSecrets(
        (type(closeReason) == "string" and closeReason ~= "") and closeReason or "websocket error"
      )
      log.warn("lark ws error", { error = safe })
      if emitFn then
        emitFn({ type = "error", error = safe, timestampMs = wallNowMs() })
      end
    end

    if emitFn then
      emitFn({ type = "disconnected", timestampMs = wallNowMs() })
    end
    log.warn("lark ws closed, scheduling reconnect", {
      code = closeCode,
      reason = scrubSecrets(closeReason or ""),
    })

    currentWs, currentEp = reconnectWebSocket(gen)
  end
end

local function connectInitialWebSocket()
  wsGeneration = wsGeneration + 1
  local gen = wsGeneration
  local deadlineMs = monoNowMs() + STARTUP_CONNECT_BUDGET_MS

  while true do
    local remaining = deadlineMs - monoNowMs()
    if remaining <= 0 then
      error(newLarkTransportError("websocket", "TimeoutError", "initial connection timed out"), 0)
    end
    local ok, ws, ep = pcall(dialWebSocketOnce, math.min(remaining, API_TIMEOUT_MS))
    if ok then
      if stopping or wsGeneration ~= gen then
        pcall(ws.close, ws, 1000, "disconnect")
        error("lark: channel is stopping", 0)
      end
      activeEndpoint = ep
      wsHandle = ws
      reconnectAttempts = 0
      reconnectBackoffMs = 1000
      if emitFn then
        emitFn({ type = "connected", identity = botIdentity, timestampMs = wallNowMs() })
      end
      task.fork(runWebSocketSupervisor, gen, ws, ep)
      return
    end
    local err = ws
    if stopping or isLarkApiError(err) or isFrameDecodeError(err) or monoNowMs() + STARTUP_RETRY_MS >= deadlineMs then
      error(err, 0)
    end
    sleepMs(STARTUP_RETRY_MS)
  end
end

-- ---------------- Outbound Delivery Helpers ----------------

local function sendSingleMessageWithRetry(opts)
  local anchor = opts.anchor
  local inThread = opts.inThread
  local parsed = opts.parsed
  local msgType = opts.msgType
  local content = opts.content
  local uuid = opts.uuid

  local attempt = 0
  while true do
    local ok, res = pcall(function()
      local apiRes
      if anchor then
        apiRes = larkRequest(
          "message_reply",
          "POST",
          "/open-apis/im/v1/messages/" .. encodeComponentStrict(anchor) .. "/reply",
          {
            body = {
              msg_type = msgType,
              content = content,
              reply_in_thread = inThread,
              uuid = uuid,
            },
            timeoutMs = API_TIMEOUT_MS,
          }
        )
      else
        apiRes = larkRequest("message_create", "POST", "/open-apis/im/v1/messages", {
          params = { receive_id_type = parsed.receiveIdType },
          body = {
            receive_id = parsed.receiveId,
            msg_type = msgType,
            content = content,
            uuid = uuid,
          },
          timeoutMs = API_TIMEOUT_MS,
        })
      end
      local data = isRecord(apiRes.data) and apiRes.data or nil
      local messageId = data and providerText(data.message_id) or nil
      if not messageId then
        error(
          newLarkApiError(anchor and "message_reply" or "message_create", {
            httpStatus = 200,
            providerCode = 0,
            message = "provider response is missing message_id",
          }),
          0
        )
      end
      local chatId = data and providerText(data.chat_id) or nil
      if parsed.kind == "dm" and parsed.openId ~= "" and chatId then
        task.fork(rememberDmChat, parsed.openId, chatId)
      end
      return { messageId = messageId, chatId = chatId }
    end)

    if ok then
      return res
    end
    if isAmbiguousDeliveryError(res) and attempt < MAX_DELIVERY_RETRIES and not stopping then
      attempt = attempt + 1
      sleepMs(500 * attempt)
    else
      error(res, 0)
    end
  end
end

local function uploadAttachmentWithRetry(prepared)
  local tokenRetried = false
  local forceRefresh = false
  local rateLimitAttempt = 0
  local transientAttempt = 0

  while true do
    local token = getTenantToken(forceRefresh)
    forceRefresh = false
    local isImage = prepared.msgType == "image"
    local uploadUrl = isImage
        and (apiBase .. "/open-apis/im/v1/images")
      or (apiBase .. "/open-apis/im/v1/files")
    local methodName = isImage and "image_upload" or "file_upload"

    local upOk, up = pcall(attachmentsMod.upload, {
      attachmentId = prepared.attachmentId,
      url = uploadUrl,
      method = "POST",
      headers = { Authorization = "Bearer " .. token },
      fieldName = isImage and "image" or "file",
      fields = isImage
          and { image_type = "message" }
        or { file_type = prepared.fileType, file_name = prepared.filename },
      filename = prepared.filename,
      contentType = prepared.contentType,
      timeoutMs = MEDIA_TIMEOUT_MS,
    })

    if not upOk then
      local transportErr = newLarkTransportError(methodName, "UploadError", scrubSecrets(errMessage(up)))
      if transientAttempt < MAX_DELIVERY_RETRIES and not stopping then
        transientAttempt = transientAttempt + 1
        sleepMs(500 * transientAttempt)
      else
        error(transportErr, 0)
      end
    else
      local upJson = nil
      if type(up.body) == "string" and up.body ~= "" then
        local decOk, dec = pcall(json.decode, up.body)
        if decOk then
          upJson = dec
        end
      end

      local providerCode = (isRecord(upJson) and isIntegerNumber(upJson.code))
          and (math.tointeger(upJson.code) or math.floor(upJson.code))
        or nil
      local providerMsg = scrubSecrets(
        (isRecord(upJson) and type(upJson.msg) == "string" and upJson.msg ~= "") and upJson.msg
          or (type(up.body) == "string" and up.body ~= "" and string.sub(up.body, 1, 120))
          or ("HTTP " .. tostring(up.status))
      )

      if up.status >= 200 and up.status < 300 and providerCode == 0 then
        local keyField = isImage and "image_key" or "file_key"
        local key = (isRecord(upJson.data)) and providerText(upJson.data[keyField]) or nil
        if not key then
          error(
            newLarkApiError(methodName, {
              httpStatus = up.status,
              providerCode = 0,
              message = "provider response is missing " .. keyField,
            }),
            0
          )
        end
        return key
      end

      local apiErr = newLarkApiError(methodName, {
        httpStatus = up.status,
        providerCode = providerCode,
        message = providerMsg,
        retryAfterMs = parseRetryAfterMs(type(up.headers) == "table" and up.headers["retry-after"] or nil),
      })

      if isTokenInvalidError(apiErr) and not tokenRetried then
        invalidateTenantToken(token)
        tokenRetried = true
        forceRefresh = true
      elseif isRateLimitError(apiErr) and rateLimitAttempt < MAX_RATE_LIMIT_RETRIES and not stopping then
        local delay = (apiErr.retryAfterMs and apiErr.retryAfterMs > 0)
            and apiErr.retryAfterMs
          or math.min(1000 * (1 << rateLimitAttempt), 15000)
        rateLimitAttempt = rateLimitAttempt + 1
        sleepMs(delay)
      elseif apiErr.httpStatus >= 500 and transientAttempt < MAX_DELIVERY_RETRIES and not stopping then
        transientAttempt = transientAttempt + 1
        sleepMs(500 * transientAttempt)
      else
        error(apiErr, 0)
      end
    end
  end
end

-- ---------------- Exported Channel Plugin Lifecycle ----------------

local function resetRuntimeState()
  cancelTimer(reconnectTimer)
  reconnectTimer = nil
  local ws = wsHandle
  wsHandle = nil
  if ws then
    pcall(ws.close, ws, 1000, "reset")
  end
  if eventChannel then
    eventChannel:close()
    eventChannel = nil
  end
  if tokenRefreshToken then
    local oldTok = tokenRefreshToken
    tokenRefreshToken = nil
    task.multi_wakeup(oldTok, false, "lark: channel disconnected")
  end
  for _, entry in pairs(pendingByToken) do
    cancelTimer(entry.timer)
    entry.timer = nil
  end
  activeEndpoint = nil
  reconnectBackoffMs = 1000
  reconnectAttempts = 0
  wsGeneration = wsGeneration + 1
  cachedToken = ""
  tokenRefreshAt = 0
  botIdentity = nil
  fragments = {}
  resetDedup()
  senderNameCache = {}
  groupNameCache = {}
  dmChatIds = {}
  topicRoots = {}
  pendingByToken = {}
  pendingByPromptId = {}
  activeCardTasks = {}
end

local function waitTrackersWithBudget(trackers, budgetMs)
  local remaining = 0
  for _, tracker in ipairs(trackers) do
    if tracker and not tracker.done then
      remaining = remaining + 1
    end
  end
  if remaining == 0 then
    return
  end
  local waitTok = {}
  local finished = false
  for _, tracker in ipairs(trackers) do
    if tracker and not tracker.done then
      task.fork(function()
        awaitTrackedTask(tracker)
        remaining = remaining - 1
        if remaining <= 0 and not finished then
          finished = true
          task.wakeup(waitTok, true)
        end
      end)
    end
  end
  pcall(task.wait, waitTok, clampDelayMs(budgetMs))
  finished = true
end

local function connect(_ctx, config, emit)
  resetRuntimeState()

  local cfg = type(config) == "table" and config or {}
  appId = trim(cfg.appId)
  local secretVal = secrets.get and secrets.get("appSecret") or nil
  appSecret = trim(secretVal or cfg.appSecret)
  if appId == "" or appSecret == "" then
    error("lark: appId and appSecret are required", 0)
  end
  registerSecret(appSecret)

  local resolved = resolveRegionAndDomain(cfg)
  apiBase = resolved.apiBase
  regionKey = resolved.regionKey
  emitFn = emit
  stopping = false

  local ok, err = pcall(function()
    local info = larkRequest("bot_info", "GET", "/open-apis/bot/v3/info", {
      timeoutMs = API_TIMEOUT_MS,
    })
    botIdentity = parseBotInfo(info)
    startEventWorker()
    connectInitialWebSocket()
  end)
  if not ok then
    stopping = true
    emitFn = nil
    resetRuntimeState()
    error(scrubSecrets(errMessage(err)), 0)
  end
end

local function send(request)
  if not botIdentity or stopping then
    error("lark: channel is not connected", 0)
  end
  local req = type(request) == "table" and request or {}
  local parsed = parseTarget(req.target)
  if parsed.botId and parsed.botId ~= "" and parsed.botId ~= botIdentity.id then
    error("lark: outbound route belongs to another bot", 0)
  end
  local inThread = parsed.kind == "thread"
  local anchor = providerMessageAnchor(req.replyToId)
  if not anchor and inThread then
    anchor = getTopicRoot(parsed.threadId)
  end
  if inThread and not anchor then
    error("lark: topic delivery requires a reply anchor", 0)
  end

  local body = (not isNull(req.body)) and tostring(req.body) or ""
  local chunks = trim(body) ~= "" and splitMarkdown(body, { limit = MAX_MARKDOWN_CODEPOINTS }) or {}

  local attIds = type(req.attachmentIds) == "table" and req.attachmentIds or {}
  if #chunks == 0 and #attIds == 0 then
    error("lark: outbound message must not be empty", 0)
  end

  -- Preflight-validate all attachments before sending any message part.
  local preparedAttachments = {}
  for _, attId in ipairs(attIds) do
    if not attachmentsMod then
      error("lark: attachments permission not granted", 0)
    end
    local meta = attachmentsMod.stat(attId)
    preparedAttachments[#preparedAttachments + 1] = validateAttachmentMeta(attId, meta)
  end

  local sentIds = {}

  for _, chunk in ipairs(chunks) do
    local uuid = generateUuid()
    local ok, sent = pcall(sendSingleMessageWithRetry, {
      anchor = anchor,
      inThread = inThread,
      parsed = parsed,
      msgType = "post",
      content = markdownPostContent(chunk),
      uuid = uuid,
    })
    if not ok then
      local safeErr = scrubSecrets(errMessage(sent))
      if #sentIds > 0 then
        return { status = "partial", ids = json.array(sentIds), error = safeErr }
      end
      if isAmbiguousDeliveryError(sent) then
        return { status = "unknown", ids = json.array(sentIds), error = safeErr }
      end
      error(safeErr, 0)
    end
    sentIds[#sentIds + 1] = sent.messageId
    -- In a thread/topic chain every part onto the previous sent message; otherwise reply only once.
    anchor = inThread and sent.messageId or nil
  end

  for _, prepared in ipairs(preparedAttachments) do
    local upOk, mediaKey = pcall(uploadAttachmentWithRetry, prepared)
    if not upOk then
      local safeErr = scrubSecrets(errMessage(mediaKey))
      if #sentIds > 0 then
        return { status = "partial", ids = json.array(sentIds), error = safeErr }
      end
      error(safeErr, 0)
    end

    local keyField = prepared.msgType == "image" and "image_key" or "file_key"
    local uuid = generateUuid()
    local sendOk, sent = pcall(sendSingleMessageWithRetry, {
      anchor = anchor,
      inThread = inThread,
      parsed = parsed,
      msgType = prepared.msgType,
      content = json.encode({ [keyField] = mediaKey }),
      uuid = uuid,
    })
    if not sendOk then
      local safeErr = scrubSecrets(errMessage(sent))
      if #sentIds > 0 then
        return { status = "partial", ids = json.array(sentIds), error = safeErr }
      end
      if isAmbiguousDeliveryError(sent) then
        return { status = "unknown", ids = json.array(sentIds), error = safeErr }
      end
      error(safeErr, 0)
    end
    sentIds[#sentIds + 1] = sent.messageId
    anchor = inThread and sent.messageId or nil
  end

  return { status = "sent", ids = json.array(sentIds) }
end

local function setTyping(_target, active, meta)
  if not active or stopping or not botIdentity then
    return
  end
  local messageId = isRecord(meta) and providerMessageAnchor(meta.messageId) or nil
  if not messageId then
    return
  end
  local ok, err = pcall(
    larkRequest,
    "message_reaction_create",
    "POST",
    "/open-apis/im/v1/messages/" .. encodeComponentStrict(messageId) .. "/reactions",
    {
      body = { reaction_type = { emoji_type = READ_MARKER_EMOJI } },
      timeoutMs = CALL_TIMEOUT_MS,
      maxRateLimitRetries = 1,
    }
  )
  if not ok then
    log.warn("lark read-marker reaction failed", {
      messageId = messageId,
      emoji = READ_MARKER_EMOJI,
      error = scrubSecrets(errMessage(err)),
    })
  end
end

local function promptInteraction(prompt)
  if not botIdentity or stopping then
    error("lark: channel is not connected", 0)
  end
  local p = type(prompt) == "table" and prompt or {}
  local promptId = providerText((not isNull(p.promptId)) and tostring(p.promptId) or "")
  if not promptId then
    error("lark: promptId is required", 0)
  end
  if pendingByPromptId[promptId] then
    error("lark: approval prompt " .. promptId .. " is already pending", 0)
  end

  local parsed = parseTarget(p.target)
  if parsed.botId and parsed.botId ~= "" and parsed.botId ~= botIdentity.id then
    error("lark: approval route belongs to another bot", 0)
  end
  local inThread = parsed.kind == "thread"
  local anchor = providerMessageAnchor(p.replyToId)
  if not anchor and inThread then
    anchor = getTopicRoot(parsed.threadId)
  end
  if inThread and not anchor then
    error("lark: approval prompt requires a topic reply anchor", 0)
  end

  local rawActions = type(p.actions) == "table" and p.actions or {}
  local actions = {}
  local actionIds = {}
  local actionLabels = {}
  for _, a in ipairs(rawActions) do
    local rawId = isRecord(a) and not isNull(a.id) and tostring(a.id) or ""
    if providerText(rawId) then
      local id = rawId
      local label = (not isNull(a.label) and tostring(a.label) ~= "") and tostring(a.label) or id
      local style = (not isNull(a.style) and tostring(a.style) ~= "") and tostring(a.style) or "default"
      actions[#actions + 1] = { id = id, label = label, style = style }
      actionIds[id] = true
      actionLabels[id] = label
    end
  end

  local token = randomBase64Url(18)
  local initialChatId = parsed.chatId
  if parsed.kind == "dm" and parsed.openId ~= "" then
    local cachedChatId = getDmChatId(parsed.openId)
    if cachedChatId ~= "" then
      initialChatId = cachedChatId
    end
  end

  local entry = {
    promptId = promptId,
    token = token,
    target = tostring(p.target),
    allowedActorId = providerText(p.allowedActorId) or "",
    title = (not isNull(p.title) and tostring(p.title) ~= "") and tostring(p.title) or "Approval required",
    body = (not isNull(p.body)) and tostring(p.body) or "",
    actions = actions,
    actionIds = actionIds,
    actionLabels = actionLabels,
    chatId = initialChatId,
    messageId = nil,
    settled = false,
    timer = nil,
  }

  pendingByToken[token] = entry
  pendingByPromptId[promptId] = entry

  local content = json.encode(buildApprovalCard(entry, nil))
  local sendOk, sent = pcall(sendSingleMessageWithRetry, {
    anchor = anchor,
    inThread = inThread,
    parsed = parsed,
    msgType = "interactive",
    content = content,
    uuid = token,
  })
  if not sendOk then
    pendingByToken[token] = nil
    pendingByPromptId[promptId] = nil
    error(scrubSecrets(errMessage(sent)), 0)
  end
  entry.messageId = sent.messageId
  if sent.chatId then
    entry.chatId = sent.chatId
  end

  local timeoutMs = tonumber(p.timeoutMs)
  if isFiniteNumber(timeoutMs) and timeoutMs > 0 and not entry.settled then
    entry.timer = task.timeout(
      clampDelayMs(timeoutMs),
      closePendingPrompt,
      entry,
      "Action rejected: approval timed out"
    )
  end
end

local function cancelInteraction(promptId, reason)
  local key = trim(promptId)
  if key == "" then
    return
  end
  local entry = pendingByPromptId[key]
  if not entry or entry.settled then
    return
  end
  local cleanReason = not isNull(reason) and trim(reason) or ""
  local feedback = cleanReason ~= ""
      and ("Action rejected: " .. cleanReason)
    or "Action rejected: the approval request was cancelled"
  closePendingPrompt(entry, feedback)
end

local function resolveDM(query, contact)
  local raw = trim(query)
  local stripped = trim(trim(raw:gsub("^[Dd][Mm]:", "")):gsub("^@+", ""))

  -- 1. Prefer contact.senderId (especially ou_... open_id).
  local contactId = isRecord(contact) and providerText(contact.senderId) or nil
  if contactId then
    if string.sub(contactId, 1, 3) == "ou_" then
      return "dm:" .. contactId
    end
    local cachedByContactId = providerText(kvGet(KV_DM_HANDLE_PREFIX .. string.lower(contactId)))
    if cachedByContactId then
      return "dm:" .. cachedByContactId
    end
    return "dm:" .. contactId
  end

  if stripped == "" then
    error("lark: empty DM query", 0)
  end

  -- 2. Direct open_id (or legacy chat_id) query.
  local validStripped = providerText(stripped)
  if validStripped and (string.sub(validStripped, 1, 3) == "ou_" or string.sub(validStripped, 1, 3) == "oc_") then
    return "dm:" .. validStripped
  end

  -- 3. Fall back to KV handle/name cache.
  local candidates = { stripped }
  if isRecord(contact) then
    if type(contact.senderHandle) == "string" and trim(contact.senderHandle) ~= "" then
      candidates[#candidates + 1] = trim(contact.senderHandle):gsub("^@+", "")
    end
    if type(contact.senderName) == "string" and trim(contact.senderName) ~= "" then
      candidates[#candidates + 1] = trim(contact.senderName)
    end
  end

  for _, cand in ipairs(candidates) do
    local clean = providerText(cand)
    if clean then
      local cached = providerText(kvGet(KV_DM_HANDLE_PREFIX .. string.lower(clean)))
      if cached then
        return "dm:" .. cached
      end
    end
  end

  error('lark: unable to resolve DM target "' .. raw .. '" (no open_id found)', 0)
end

local function disconnect()
  stopping = true
  wsGeneration = wsGeneration + 1

  cancelTimer(reconnectTimer)
  reconnectTimer = nil

  if eventChannel then
    eventChannel:close()
    eventChannel = nil
  end

  -- Close all pending interactive cards within a bounded budget.
  local pending = {}
  for _, entry in pairs(pendingByToken) do
    pending[#pending + 1] = entry
  end
  if #pending > 0 then
    local closeTrackers = {}
    for _, entry in ipairs(pending) do
      local tracker = { done = false, token = {} }
      closeTrackers[#closeTrackers + 1] = tracker
      task.fork(function()
        pcall(closePendingPrompt, entry, "Action rejected: the Lark channel stopped")
        tracker.done = true
        task.multi_wakeup(tracker.token)
      end)
    end
    waitTrackersWithBudget(closeTrackers, DISCONNECT_CLOSE_BUDGET_MS)
  end

  local remainingCardTrackers = {}
  for tracker in pairs(activeCardTasks) do
    remainingCardTrackers[#remainingCardTrackers + 1] = tracker
  end
  if #remainingCardTrackers > 0 then
    waitTrackersWithBudget(remainingCardTrackers, 2000)
  end

  local ws = wsHandle
  wsHandle = nil
  if ws then
    pcall(ws.close, ws, 1000, "disconnect")
  end

  local prevEmit = emitFn
  emitFn = nil
  resetRuntimeState()

  if prevEmit then
    prevEmit({ type = "disconnected", timestampMs = wallNowMs() })
  end
end

return {
  connect = connect,
  send = send,
  disconnect = disconnect,
  setTyping = setTyping,
  promptInteraction = promptInteraction,
  cancelInteraction = cancelInteraction,
  resolveDM = resolveDM,
  conversationKey = conversationKey,
}
