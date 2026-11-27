-- Outbound Markdown chunking, attachment classification/validation, and error classification.
-- Ported from Bazaar Compute 0.3 bazaar_contrib_lark/markdown.py & outbound.py.

local json = require("bazaar.json")

local MAX_MARKDOWN_CODEPOINTS = 3500
local MAX_FILENAME_BYTES = 256
local MAX_IMAGE_BYTES = 10 * 1024 * 1024
local MAX_FILE_BYTES = 30 * 1024 * 1024

local FATAL_AUTH_HTTP_STATUSES = { [401] = true, [403] = true }
local FATAL_AUTH_PROVIDER_CODES = {
  [403] = true,
  [514] = true,
  [1000040344] = true,
  [1000040350] = true,
}
local RATE_LIMIT_PROVIDER_CODES = { [99991400] = true }
local TOKEN_INVALID_PROVIDER_CODES = { [99991663] = true }

local IMAGE_SUFFIXES = {
  [".bmp"] = true,
  [".gif"] = true,
  [".ico"] = true,
  [".jpeg"] = true,
  [".jpg"] = true,
  [".png"] = true,
  [".tif"] = true,
  [".tiff"] = true,
  [".webp"] = true,
}

local AUDIO_SUFFIXES = { [".opus"] = true }
local VIDEO_SUFFIXES = { [".mp4"] = true }

local DOCUMENT_FILE_TYPES = {
  [".doc"] = "doc",
  [".docx"] = "doc",
  [".pdf"] = "pdf",
  [".ppt"] = "ppt",
  [".pptx"] = "ppt",
  [".xls"] = "xls",
  [".xlsx"] = "xls",
}

local EXTENSION_TO_MIME = {
  [".bmp"] = "image/bmp",
  [".gif"] = "image/gif",
  [".ico"] = "image/x-icon",
  [".jpeg"] = "image/jpeg",
  [".jpg"] = "image/jpeg",
  [".png"] = "image/png",
  [".tif"] = "image/tiff",
  [".tiff"] = "image/tiff",
  [".webp"] = "image/webp",
  [".opus"] = "audio/opus",
  [".mp3"] = "audio/mpeg",
  [".wav"] = "audio/wav",
  [".ogg"] = "audio/ogg",
  [".mp4"] = "video/mp4",
  [".pdf"] = "application/pdf",
  [".doc"] = "application/msword",
  [".docx"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
  [".ppt"] = "application/vnd.ms-powerpoint",
  [".pptx"] = "application/vnd.openxmlformats-officedocument.presentationml.presentation",
  [".xls"] = "application/vnd.ms-excel",
  [".xlsx"] = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  [".txt"] = "text/plain",
  [".md"] = "text/markdown",
  [".json"] = "application/json",
  [".csv"] = "text/csv",
  [".zip"] = "application/zip",
}

local function trim(s)
  if type(s) ~= "string" then
    return ""
  end
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function utf8Bytes(text)
  if text == nil then
    return 0
  end
  return #tostring(text)
end

local function toCodePoints(text)
  local s = text == nil and "" or tostring(text)
  local cps = {}
  if s == "" then
    return cps
  end
  if utf8.len(s) ~= nil then
    local prev = 1
    local len = #s
    for pos in utf8.codes(s) do
      if pos > 1 then
        cps[#cps + 1] = string.sub(s, prev, pos - 1)
      end
      prev = pos
    end
    if prev <= len then
      cps[#cps + 1] = string.sub(s, prev, len)
    end
    return cps
  end
  -- Fallback for strings containing invalid UTF-8 bytes.
  local len = #s
  local i = 1
  while i <= len do
    local b = string.byte(s, i)
    local charLen = 1
    if b >= 0xF0 and i + 3 <= len then
      local b2, b3, b4 = string.byte(s, i + 1, i + 3)
      if b2 >= 0x80 and b2 <= 0xBF and b3 >= 0x80 and b3 <= 0xBF and b4 >= 0x80 and b4 <= 0xBF then
        charLen = 4
      end
    elseif b >= 0xE0 and b < 0xF0 and i + 2 <= len then
      local b2, b3 = string.byte(s, i + 1, i + 2)
      if b2 >= 0x80 and b2 <= 0xBF and b3 >= 0x80 and b3 <= 0xBF then
        charLen = 3
      end
    elseif b >= 0xC2 and b < 0xE0 and i + 1 <= len then
      local b2 = string.byte(s, i + 1)
      if b2 >= 0x80 and b2 <= 0xBF then
        charLen = 2
      end
    end
    cps[#cps + 1] = string.sub(s, i, i + charLen - 1)
    i = i + charLen
  end
  return cps
end

local function codePoints(text)
  local s = text == nil and "" or tostring(text)
  if s == "" then
    return 0
  end
  local n = utf8.len(s)
  if n ~= nil then
    return n
  end
  return #toCodePoints(s)
end

local function splitLinesKeepEnds(segment)
  local lines = {}
  local startPos = 1
  local len = #segment
  local i = 1
  while i <= len do
    local b = string.byte(segment, i)
    if b == 13 then -- '\r'
      if i + 1 <= len and string.byte(segment, i + 1) == 10 then
        i = i + 1
      end
      lines[#lines + 1] = string.sub(segment, startPos, i)
      startPos = i + 1
    elseif b == 10 then -- '\n'
      lines[#lines + 1] = string.sub(segment, startPos, i)
      startPos = i + 1
    end
    i = i + 1
  end
  if startPos <= len then
    lines[#lines + 1] = string.sub(segment, startPos)
  end
  return lines
end

local function countLeadingByte(str, byteVal)
  local i = 1
  local len = #str
  while i <= len and string.byte(str, i) == byteVal do
    i = i + 1
  end
  return i - 1
end

local function endsWithLineBreak(s)
  local len = #s
  if len == 0 then
    return false
  end
  local last = string.byte(s, len)
  return last == 10 or last == 13
end

local function advanceFence(fence, segment, opts)
  local initialLineBoundary = opts.initialLineBoundary
  local terminalLineComplete = opts.terminalLineComplete
  local lines = splitLinesKeepEnds(segment)
  local current = fence
  local numLines = #lines
  for index = 1, numLines do
    local line = lines[index]
    local endsLine = endsWithLineBreak(line)
    local complete = (index > 1 or initialLineBoundary)
      and (endsLine or (index == numLines and terminalLineComplete))
    if complete then
      local stripped = trim(line)
      if stripped ~= "" then
        if current == nil then
          local firstByte = string.byte(stripped, 1)
          if firstByte == 96 or firstByte == 126 then -- '`' or '~'
            local markerLength = countLeadingByte(stripped, firstByte)
            if markerLength >= 3 then
              local markerChar = string.char(firstByte)
              local marker = string.rep(markerChar, markerLength)
              local info = string.sub(stripped, markerLength + 1)
              if not (firstByte == 96 and string.find(info, "`", 1, true) ~= nil) then
                current = { marker = marker, opening = stripped }
              end
            end
          end
        else
          local expectedByte = string.byte(current.marker, 1)
          if string.byte(stripped, 1) == expectedByte then
            local markerLength = countLeadingByte(stripped, expectedByte)
            if markerLength >= #current.marker and trim(string.sub(stripped, markerLength + 1)) == "" then
              current = nil
            end
          end
        end
      end
    end
  end
  return current
end

local function closingSuffix(content, fence)
  if not fence then
    return ""
  end
  local separator = endsWithLineBreak(content) and "" or "\n"
  return separator .. fence.marker
end

local function isWhitespaceChar(ch)
  if #ch == 1 then
    return ch:match("^%s$") ~= nil
  end
  return ch == "\u{00A0}" or ch == "\u{3000}"
end

local function preferredBoundary(cps, minimum, endIdx)
  -- Search for "\n\n"
  for i = endIdx - 2, minimum, -1 do
    if cps[i] == "\n" and cps[i + 1] == "\n" then
      return i + 2
    end
  end
  -- Search for "\n"
  for i = endIdx - 1, minimum, -1 do
    if cps[i] == "\n" then
      return i + 1
    end
  end
  -- Search for whitespace
  for i = endIdx - 1, minimum, -1 do
    if isWhitespaceChar(cps[i]) then
      return i + 1
    end
  end
  return nil
end

local function splitMarkdown(content, opts)
  opts = type(opts) == "table" and opts or {}
  local limit = opts.limit
  if limit == nil then
    limit = MAX_MARKDOWN_CODEPOINTS
  end
  local measure = opts.measure or codePoints

  if type(limit) ~= "number" or math.type(limit) ~= "integer" or limit <= 0 then
    error("lark: markdown split limit must be a positive integer", 0)
  end

  local text = content == nil and "" or tostring(content)
  if text == "" then
    return { text }
  end

  local cps = toCodePoints(text)
  local n = #cps
  local chunks = {}
  local cursor = 1
  local fence = nil

  while cursor <= n do
    local prefix = fence and (fence.opening .. "\n") or ""
    local endIdx = cursor
    local size = measure(prefix)
    while endIdx <= n do
      local step = measure(cps[endIdx])
      if size + step > limit then
        break
      end
      size = size + step
      endIdx = endIdx + 1
    end
    if endIdx == cursor then
      error("lark: markdown fence overhead exceeds provider limit", 0)
    end

    local lineStart = fence ~= nil
      or cursor == 1
      or cps[cursor - 1] == "\r"
      or cps[cursor - 1] == "\n"
    local nextFence
    local suffix
    while true do
      local segment = table.concat(cps, "", cursor, endIdx - 1)
      nextFence = advanceFence(fence, segment, {
        initialLineBoundary = lineStart,
        terminalLineComplete = endIdx > n or cps[endIdx] == "\r" or cps[endIdx] == "\n",
      })
      suffix = closingSuffix(prefix .. segment, nextFence)
      if measure(prefix .. segment .. suffix) <= limit then
        break
      end
      endIdx = endIdx - 1
      if endIdx == cursor then
        error("lark: markdown fence closure exceeds provider limit", 0)
      end
    end

    if endIdx <= n then
      local minimum = cursor + math.max(1, (endIdx - cursor) // 2)
      local preferred = preferredBoundary(cps, minimum, endIdx)
      if preferred ~= nil then
        local prefSegment = table.concat(cps, "", cursor, preferred - 1)
        local prefFence = advanceFence(fence, prefSegment, {
          initialLineBoundary = lineStart,
          terminalLineComplete = preferred > n or cps[preferred] == "\r" or cps[preferred] == "\n",
        })
        local prefSuffix = closingSuffix(prefix .. prefSegment, prefFence)
        if measure(prefix .. prefSegment .. prefSuffix) <= limit then
          endIdx = preferred
          nextFence = prefFence
          suffix = prefSuffix
        end
      end
    end

    chunks[#chunks + 1] = prefix .. table.concat(cps, "", cursor, endIdx - 1) .. suffix
    cursor = endIdx
    fence = nextFence
  end

  return chunks
end

local function markdownPostContent(markdown)
  local md = markdown == nil and "" or tostring(markdown)
  local row = json.array({ { tag = "md", text = md } })
  local content = json.array({ row })
  return json.encode({
    zh_cn = {
      title = "",
      content = content,
    },
  })
end

local function fileSuffix(filename)
  local raw = filename ~= nil and tostring(filename) or ""
  local base = raw:match("[^/\\]*$") or ""
  local dot = base:match(".*()%.")
  if not dot or dot <= 1 then
    return ""
  end
  return string.lower(string.sub(base, dot))
end

local function guessMimeType(filename)
  return EXTENSION_TO_MIME[fileSuffix(filename)] or "application/octet-stream"
end

local function classifyAttachment(filename)
  local suffix = fileSuffix(filename)
  if IMAGE_SUFFIXES[suffix] then
    return { msgType = "image", fileType = "image", maxBytes = MAX_IMAGE_BYTES }
  end
  if AUDIO_SUFFIXES[suffix] then
    return { msgType = "audio", fileType = "opus", maxBytes = MAX_FILE_BYTES }
  end
  if VIDEO_SUFFIXES[suffix] then
    return { msgType = "media", fileType = "mp4", maxBytes = MAX_FILE_BYTES }
  end
  local docType = DOCUMENT_FILE_TYPES[suffix] or "stream"
  return { msgType = "file", fileType = docType, maxBytes = MAX_FILE_BYTES }
end

local function validateAttachmentMeta(attId, meta)
  if type(meta) ~= "table" then
    error("lark: missing metadata for attachment " .. tostring(attId), 0)
  end
  local rawName = type(meta.filename) == "string" and meta.filename or ""
  local filename = trim(rawName)
  if filename == "" then
    filename = "file"
  end
  if utf8Bytes(filename) > MAX_FILENAME_BYTES then
    error("lark: attachment filename exceeds 256 UTF-8 bytes: " .. filename, 0)
  end
  local size = tonumber(meta.size)
  if not size or size ~= size or size == math.huge or size == -math.huge or size <= 0 then
    error("lark: attachment must not be empty: " .. filename, 0)
  end
  local classified = classifyAttachment(filename)
  if size > classified.maxBytes then
    local limitMiB = math.floor(classified.maxBytes / (1024 * 1024) + 0.5)
    error(
      "lark: " .. classified.msgType .. " attachment exceeds " .. tostring(limitMiB) .. " MiB limit: " .. filename,
      0
    )
  end
  local ct = type(meta.contentType) == "string" and trim(meta.contentType) or ""
  if ct == "" then
    ct = guessMimeType(filename)
  end
  return {
    attachmentId = attId,
    filename = filename,
    contentType = ct,
    size = size,
    msgType = classified.msgType,
    fileType = classified.fileType,
  }
end

local ErrorMT = {
  __tostring = function(self)
    return self.message or self.name or "lark error"
  end,
}

local function newLarkApiError(method, opts)
  opts = type(opts) == "table" and opts or {}
  local httpStatus = opts.httpStatus or 0
  local providerCode = opts.providerCode
  local retryAfterMs = opts.retryAfterMs or 0
  local code = providerCode ~= nil and providerCode or httpStatus
  local rawMsg = opts.message
  if rawMsg == nil or rawMsg == "" then
    rawMsg = "provider rejected request"
  end
  local cleanMsg = string.sub(tostring(rawMsg), 1, 256)
  return setmetatable({
    name = "LarkApiError",
    method = method,
    httpStatus = httpStatus,
    providerCode = providerCode,
    providerMessage = cleanMsg,
    retryAfterMs = retryAfterMs,
    message = "lark " .. tostring(method) .. " failed (" .. tostring(code) .. "): " .. cleanMsg,
  }, ErrorMT)
end

local function newLarkTransportError(method, errorKind, detail)
  local msg = (detail ~= nil and detail ~= "")
      and (tostring(errorKind) .. ": " .. tostring(detail))
    or tostring(errorKind)
  return setmetatable({
    name = "LarkTransportError",
    method = method,
    errorKind = errorKind,
    message = "lark " .. tostring(method) .. " transport failed: " .. msg,
  }, ErrorMT)
end

local function isLarkApiError(err)
  return type(err) == "table" and err.name == "LarkApiError"
end

local function isLarkTransportError(err)
  return type(err) == "table" and err.name == "LarkTransportError"
end

local function isFatalAuthError(err)
  if not isLarkApiError(err) then
    return false
  end
  return FATAL_AUTH_HTTP_STATUSES[err.httpStatus] == true
    or (err.providerCode ~= nil and FATAL_AUTH_PROVIDER_CODES[err.providerCode] == true)
end

local function isRateLimitError(err)
  if not isLarkApiError(err) then
    return false
  end
  return err.httpStatus == 429
    or (err.providerCode ~= nil and RATE_LIMIT_PROVIDER_CODES[err.providerCode] == true)
end

local function isTokenInvalidError(err)
  if not isLarkApiError(err) then
    return false
  end
  return err.providerCode ~= nil and TOKEN_INVALID_PROVIDER_CODES[err.providerCode] == true
end

local function isAmbiguousDeliveryError(err)
  if isLarkTransportError(err) then
    return true
  end
  if isLarkApiError(err) then
    return (err.httpStatus or 0) >= 500 or err.providerCode == 0 or err.providerCode == nil
  end
  return false
end

return {
  MAX_MARKDOWN_CODEPOINTS = MAX_MARKDOWN_CODEPOINTS,
  MAX_FILENAME_BYTES = MAX_FILENAME_BYTES,
  MAX_IMAGE_BYTES = MAX_IMAGE_BYTES,
  MAX_FILE_BYTES = MAX_FILE_BYTES,
  FATAL_AUTH_HTTP_STATUSES = FATAL_AUTH_HTTP_STATUSES,
  FATAL_AUTH_PROVIDER_CODES = FATAL_AUTH_PROVIDER_CODES,
  RATE_LIMIT_PROVIDER_CODES = RATE_LIMIT_PROVIDER_CODES,
  TOKEN_INVALID_PROVIDER_CODES = TOKEN_INVALID_PROVIDER_CODES,
  utf8Bytes = utf8Bytes,
  codePoints = codePoints,
  advanceFence = advanceFence,
  closingSuffix = closingSuffix,
  splitMarkdown = splitMarkdown,
  markdownPostContent = markdownPostContent,
  guessMimeType = guessMimeType,
  classifyAttachment = classifyAttachment,
  validateAttachmentMeta = validateAttachmentMeta,
  newLarkApiError = newLarkApiError,
  newLarkTransportError = newLarkTransportError,
  isLarkApiError = isLarkApiError,
  isLarkTransportError = isLarkTransportError,
  isFatalAuthError = isFatalAuthError,
  isRateLimitError = isRateLimitError,
  isTokenInvalidError = isTokenInvalidError,
  isAmbiguousDeliveryError = isAmbiguousDeliveryError,
}
