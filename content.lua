-- Lark inbound message content projection, mention normalization, and resource helpers.
-- Ported from Bazaar Compute 0.3 bazaar_contrib_lark/content.py & attachments.py.

local json = require("bazaar.json")

local RESOURCE_TYPES = {
  image = true,
  file = true,
  audio = true,
  media = true,
  sticker = true,
}

local DOWNLOAD_TYPES = {
  image = "image",
  file = "file",
  audio = "file",
  media = "file",
}

local INLINE_TEXT_TAGS = {
  text = true,
  code = true,
  pre = true,
  del = true,
  bold = true,
  underline = true,
  italic = true,
  md = true,
}

local BLOCK_TAGS = {
  title = true,
  paragraph = true,
  p = true,
  li = true,
  list = true,
  blockquote = true,
  quote = true,
}

local MIME_TO_EXTENSION = {
  ["image/png"] = ".png",
  ["image/jpeg"] = ".jpg",
  ["image/jpg"] = ".jpg",
  ["image/gif"] = ".gif",
  ["image/webp"] = ".webp",
  ["image/bmp"] = ".bmp",
  ["image/tiff"] = ".tiff",
  ["image/x-icon"] = ".ico",
  ["image/svg+xml"] = ".svg",
  ["audio/mpeg"] = ".mp3",
  ["audio/mp3"] = ".mp3",
  ["audio/ogg"] = ".ogg",
  ["audio/opus"] = ".opus",
  ["audio/wav"] = ".wav",
  ["audio/x-wav"] = ".wav",
  ["audio/mp4"] = ".m4a",
  ["audio/aac"] = ".aac",
  ["audio/flac"] = ".flac",
  ["video/mp4"] = ".mp4",
  ["video/quicktime"] = ".mov",
  ["video/webm"] = ".webm",
  ["video/x-msvideo"] = ".avi",
  ["application/pdf"] = ".pdf",
  ["application/zip"] = ".zip",
  ["application/gzip"] = ".gz",
  ["application/json"] = ".json",
  ["application/msword"] = ".doc",
  ["application/vnd.openxmlformats-officedocument.wordprocessingml.document"] = ".docx",
  ["application/vnd.ms-excel"] = ".xls",
  ["application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"] = ".xlsx",
  ["application/vnd.ms-powerpoint"] = ".ppt",
  ["application/vnd.openxmlformats-officedocument.presentationml.presentation"] = ".pptx",
  ["text/plain"] = ".txt",
  ["text/markdown"] = ".md",
  ["text/csv"] = ".csv",
  ["text/html"] = ".html",
}

local POST_LOCALE_ORDER = { "zh_cn", "en_us", "ja_jp", "ko_kr" }
local RESOURCE_KEY_FIELDS = {
  "file_key",
  "image_key",
  "audio_key",
  "media_key",
  "sticker_key",
  "resource_key",
  "key",
}
local RESOURCE_NAME_FIELDS = { "file_name", "name", "filename", "alt", "text" }

local jsonArrayMeta = json.array and getmetatable(json.array()) or nil

local function jsonArray(tbl)
  local list = tbl or {}
  if json.array then
    return json.array(list)
  end
  return list
end

local function isNull(v)
  return v == nil or (json.null ~= nil and v == json.null)
end

local function trim(s)
  if type(s) ~= "string" then
    return ""
  end
  return (s:gsub("^%s+", ""):gsub("%s+$", ""))
end

local function isArray(value)
  if type(value) ~= "table" then
    return false
  end
  if jsonArrayMeta ~= nil and getmetatable(value) == jsonArrayMeta then
    return true
  end
  local count = 0
  local maxIdx = 0
  for k in pairs(value) do
    if type(k) ~= "number" or math.type(k) ~= "integer" or k < 1 then
      return false
    end
    count = count + 1
    if k > maxIdx then
      maxIdx = k
    end
  end
  return count > 0 and maxIdx == count
end

local function isRecord(value)
  return type(value) == "table" and not isArray(value)
end

local function providerText(value)
  if type(value) ~= "string" or value == "" or value:find("[\r\n]") ~= nil then
    return nil
  end
  return value
end

local function providerTimeMs(value)
  local t = type(value)
  if t == "number" then
    if value ~= value or value == math.huge or value == -math.huge then
      return nil
    end
    if math.floor(value) == value and value >= 0 then
      return math.tointeger(value) or math.floor(value)
    end
    return nil
  end
  if t == "string" and value:match("^%d+$") ~= nil then
    local n = math.tointeger(value)
    if n ~= nil and n >= 0 then
      return n
    end
  end
  return nil
end

local function isSafeFilename(value)
  if type(value) ~= "string" or value == "" or #value > 255 then
    return false
  end
  if value:find("[/\\]") ~= nil then
    return false
  end
  for i = 1, #value do
    local code = string.byte(value, i)
    if code < 32 or code == 127 then
      return false
    end
  end
  return true
end

local function safeFilename(value, fallback)
  if isSafeFilename(value) then
    return value
  end
  return fallback
end

local function parseMentions(value)
  local mentions = {}
  if not isArray(value) then
    return mentions
  end
  for _, item in ipairs(value) do
    if isRecord(item) then
      local key = providerText(item.key)
      if key then
        local openId = isRecord(item.id) and providerText(item.id.open_id) or nil
        local displayName = providerText(item.name) or openId or key
        mentions[key] = {
          key = key,
          openId = openId,
          displayName = displayName,
        }
      end
    end
  end
  return mentions
end

local function decodeContent(content)
  if isRecord(content) then
    return content, false
  end
  if type(content) ~= "string" then
    return {}, true
  end
  local ok, decoded = pcall(json.decode, content)
  if ok and isRecord(decoded) then
    return decoded, false
  end
  return {}, true
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

local function replaceMentions(text, mentions, botOpenId, state)
  local rendered = text
  if type(mentions) ~= "table" or next(mentions) == nil then
    return rendered
  end
  local ordered = {}
  for key, mention in pairs(mentions) do
    ordered[#ordered + 1] = { key = key, mention = mention }
  end
  table.sort(ordered, function(a, b)
    if #a.key ~= #b.key then
      return #a.key > #b.key
    end
    return a.key < b.key
  end)
  for _, entry in ipairs(ordered) do
    local key = entry.key
    local mention = entry.mention
    if string.find(rendered, key, 1, true) ~= nil then
      if botOpenId and botOpenId ~= "" and mention.openId == botOpenId then
        if state then
          state.mentionedBot = true
        end
        rendered = replaceLiteral(rendered, key, "")
      else
        rendered = replaceLiteral(rendered, key, "@" .. mention.displayName)
      end
    end
  end
  return rendered
end

local function selectPostLocale(content)
  if not isRecord(content) then
    return content
  end
  if not isNull(content.title) or not isNull(content.content) then
    return content
  end
  for _, locale in ipairs(POST_LOCALE_ORDER) do
    local candidate = content[locale]
    if isRecord(candidate) or isArray(candidate) then
      return candidate
    end
  end
  local keys = {}
  for k in pairs(content) do
    keys[#keys + 1] = tostring(k)
  end
  table.sort(keys)
  for _, k in ipairs(keys) do
    local candidate = content[k]
    if isRecord(candidate) or isArray(candidate) then
      return candidate
    end
  end
  return content
end

local function resourceTypeForTag(tag)
  if tag == "img" or tag == "image" then
    return "image"
  end
  if tag == "video" then
    return "media"
  end
  if RESOURCE_TYPES[tag] then
    return tag
  end
  return nil
end

local function extractResource(value, resourceType)
  local mapping = isRecord(value) and value or {}
  local fileKey = nil
  for _, field in ipairs(RESOURCE_KEY_FIELDS) do
    local candidate = mapping[field]
    if type(candidate) == "string" and candidate ~= "" then
      fileKey = candidate
      break
    end
  end
  local rawName = nil
  for _, field in ipairs(RESOURCE_NAME_FIELDS) do
    local candidate = mapping[field]
    if type(candidate) == "string" and candidate ~= "" then
      rawName = candidate
      break
    end
  end
  local explicitName = isSafeFilename(rawName) and rawName or ""
  local name = explicitName ~= "" and explicitName or resourceType
  local mediaType = providerText(mapping.mime_type)
  return {
    fileKey = fileKey,
    resourceType = resourceType,
    name = name,
    explicitName = explicitName,
    mediaType = mediaType,
    placeholder = "[" .. resourceType .. ": " .. name .. "]",
  }
end

local function renderMentionNode(node, mentions, botOpenId, state)
  local key = node.key
  local mention = (type(key) == "string" and type(mentions) == "table") and mentions[key] or nil
  if not mention then
    local openId = (type(node.user_id) == "string" and node.user_id ~= "") and node.user_id
      or (type(node.open_id) == "string" and node.open_id or nil)
    if type(openId) == "string" and openId ~= "" then
      if type(mentions) == "table" then
        for _, item in pairs(mentions) do
          if item.openId == openId then
            mention = item
            break
          end
        end
      end
      if not mention then
        if botOpenId and botOpenId ~= "" and openId == botOpenId then
          if state then
            state.mentionedBot = true
          end
          return ""
        end
        return "@" .. openId
      end
    end
  end
  if mention then
    if botOpenId and botOpenId ~= "" and mention.openId == botOpenId then
      if state then
        state.mentionedBot = true
      end
      return ""
    end
    return "@" .. mention.displayName
  end
  if type(node.text) == "string" then
    return node.text
  end
  return "@user"
end

local renderPost

local function renderTag(node, tag, ctx)
  if INLINE_TEXT_TAGS[tag] then
    local text = type(node.text) == "string" and node.text or ""
    return replaceMentions(text, ctx.mentions, ctx.botOpenId, ctx)
  end
  if tag == "a" or tag == "link" then
    local labelSource = node.text
    if isNull(labelSource) or labelSource == "" then
      labelSource = node.content
    end
    if isNull(labelSource) then
      labelSource = ""
    end
    local label = renderPost(labelSource, ctx)
    local href = node.href
    if type(href) ~= "string" or href == "" then
      href = node.url
    end
    if type(href) == "string" and href ~= "" then
      if label ~= "" then
        return label .. " (" .. href .. ")"
      end
      return href
    end
    return label
  end
  if tag == "at" then
    return renderMentionNode(node, ctx.mentions, ctx.botOpenId, ctx)
  end
  local resourceType = resourceTypeForTag(tag)
  if resourceType then
    local resource = extractResource(node, resourceType)
    ctx.resources[#ctx.resources + 1] = resource
    return resource.placeholder
  end
  local children = node.content
  if isNull(children) then
    children = node.children
  end
  if isNull(children) then
    children = node.text
  end
  local rendered = renderPost(children, ctx)
  if BLOCK_TAGS[tag] then
    return rendered .. "\n"
  end
  return rendered
end

renderPost = function(value, ctx)
  if type(value) == "string" then
    return replaceMentions(value, ctx.mentions, ctx.botOpenId, ctx)
  end
  if isArray(value) then
    local parts = {}
    for _, child in ipairs(value) do
      parts[#parts + 1] = renderPost(child, ctx)
    end
    return table.concat(parts)
  end
  if not isRecord(value) then
    return ""
  end
  if type(value.tag) == "string" then
    return renderTag(value, value.tag, ctx)
  end
  local parts = {}
  if not isNull(value.title) then
    parts[#parts + 1] = renderPost(value.title, ctx)
  end
  if isArray(value.content) then
    local rows = {}
    for _, row in ipairs(value.content) do
      rows[#rows + 1] = renderPost(row, ctx)
    end
    parts[#parts + 1] = table.concat(rows, "\n")
  elseif not isNull(value.content) then
    parts[#parts + 1] = renderPost(value.content, ctx)
  end
  if #parts > 0 then
    return table.concat(parts, "\n")
  end
  if type(value.text) == "string" then
    return replaceMentions(value.text, ctx.mentions, ctx.botOpenId, ctx)
  end
  return ""
end

local function uniqueResources(resources)
  local unique = {}
  local seen = {}
  for _, r in ipairs(resources) do
    local key = (r.fileKey or "") .. "\0" .. r.resourceType
    if not seen[key] then
      seen[key] = true
      unique[#unique + 1] = r
    end
  end
  return jsonArray(unique)
end

local function projectContent(messageType, content, opts)
  opts = type(opts) == "table" and opts or {}
  local mentions = type(opts.mentions) == "table" and opts.mentions or {}
  local botOpenId = type(opts.botOpenId) == "string" and opts.botOpenId or ""

  local value, contentError = decodeContent(content)
  local state = {
    mentions = mentions,
    botOpenId = botOpenId,
    resources = {},
    mentionedBot = false,
  }

  if botOpenId ~= "" then
    for _, m in pairs(mentions) do
      if m.openId == botOpenId then
        state.mentionedBot = true
        break
      end
    end
  end

  if messageType == "text" then
    local text = isRecord(value) and value.text or nil
    if contentError or type(text) ~= "string" then
      return {
        messageType = "text",
        body = "[invalid lark content: text]",
        resources = jsonArray({}),
        contentError = true,
        mentionedBot = state.mentionedBot,
      }
    end
    return {
      messageType = "text",
      body = trim(replaceMentions(text, mentions, botOpenId, state)),
      resources = jsonArray({}),
      contentError = false,
      mentionedBot = state.mentionedBot,
    }
  end

  if messageType == "post" then
    if contentError then
      return {
        messageType = "post",
        body = "[invalid lark content: post]",
        resources = jsonArray({}),
        contentError = true,
        mentionedBot = state.mentionedBot,
      }
    end
    local body = trim(renderPost(selectPostLocale(value), state))
    return {
      messageType = "post",
      body = body ~= "" and body or "[lark post]",
      resources = uniqueResources(state.resources),
      contentError = false,
      mentionedBot = state.mentionedBot,
    }
  end

  if RESOURCE_TYPES[messageType] or messageType == "video" then
    if contentError then
      return {
        messageType = messageType,
        body = "[invalid lark content: " .. messageType .. "]",
        resources = jsonArray({}),
        contentError = true,
        mentionedBot = state.mentionedBot,
      }
    end
    local resType = messageType == "video" and "media" or messageType
    local resource = extractResource(value, resType)
    return {
      messageType = messageType,
      body = resource.placeholder,
      resources = jsonArray({ resource }),
      contentError = false,
      mentionedBot = state.mentionedBot,
    }
  end

  return {
    messageType = "unsupported:" .. tostring(messageType),
    body = "[unsupported lark message type: " .. tostring(messageType) .. "]",
    resources = jsonArray({}),
    contentError = contentError,
    mentionedBot = state.mentionedBot,
  }
end

local function isSuffix(suffix)
  return type(suffix) == "string"
    and #suffix > 1
    and #suffix <= 11
    and suffix:match("^%.[A-Za-z0-9]+$") ~= nil
end

local function extractSuffix(name)
  if type(name) ~= "string" then
    return ""
  end
  local dot = name:match(".*()%.")
  if not dot or dot <= 1 then
    return ""
  end
  local cand = string.sub(name, dot)
  return isSuffix(cand) and cand or ""
end

local function fallbackContentType(resource)
  if type(resource) == "table" then
    if type(resource.mediaType) == "string" and resource.mediaType ~= "" then
      return resource.mediaType
    end
    if resource.resourceType == "audio" then
      return "audio/mpeg"
    end
  end
  return ""
end

local function resourceName(opts)
  local eventName = opts.eventName
  local responseName = opts.responseName
  local mediaType = opts.mediaType
  local resourceType = opts.resourceType

  local name
  if extractSuffix(eventName) ~= "" then
    name = eventName
  elseif responseName and extractSuffix(responseName) ~= "" then
    name = responseName
  elseif eventName ~= resourceType or not responseName then
    name = eventName
  else
    name = responseName
  end
  if extractSuffix(name) ~= "" then
    return name
  end

  local rawMedia = (not isNull(mediaType)) and tostring(mediaType) or ""
  local semi = string.find(rawMedia, ";", 1, true)
  local declared = string.lower(trim(semi and string.sub(rawMedia, 1, semi - 1) or rawMedia))
  local suffix = ""
  if declared ~= "" and string.sub(declared, -13) ~= "/octet-stream" then
    suffix = MIME_TO_EXTENSION[declared] or ""
  end
  if not isSuffix(suffix) and resourceType == "audio" then
    suffix = ".mp3"
  end
  if isSuffix(suffix) then
    return name .. suffix
  end
  return name
end

return {
  RESOURCE_TYPES = RESOURCE_TYPES,
  DOWNLOAD_TYPES = DOWNLOAD_TYPES,
  isArray = isArray,
  isRecord = isRecord,
  providerText = providerText,
  providerTimeMs = providerTimeMs,
  isSafeFilename = isSafeFilename,
  safeFilename = safeFilename,
  parseMentions = parseMentions,
  projectContent = projectContent,
  fallbackContentType = fallbackContentType,
  resourceName = resourceName,
}
