-- Pure zero-dependency protobuf encoder/decoder for Lark WebSocket Frame messages.
-- Uses Lua 5.5 native 64-bit integers, bitwise operators, and 8-bit binary-safe strings.

local FrameDecodeErrorMT = {
  __tostring = function(self)
    return self.message or "lark frame decode error"
  end,
}

local function newFrameDecodeError(message)
  return setmetatable({
    name = "FrameDecodeError",
    message = tostring(message or "lark frame decode error"),
  }, FrameDecodeErrorMT)
end

local function isFrameDecodeError(err)
  return type(err) == "table" and err.name == "FrameDecodeError"
end

local function raiseDecodeError(message)
  error(newFrameDecodeError(message), 0)
end

local function toBinaryString(raw)
  if type(raw) == "string" then
    return raw
  end
  raiseDecodeError("lark frame: payload must be binary")
end

local function toInt64(value)
  if value == nil or value == 0 then
    return 0
  end
  local t = type(value)
  if t == "number" then
    if math.type(value) == "integer" then
      return value
    end
    local i = math.tointeger(value)
    if i ~= nil then
      return i
    end
    if value ~= value or value == math.huge or value == -math.huge then
      return 0
    end
    return math.floor(value)
  end
  if t == "string" then
    local s = value:gsub("^%s+", ""):gsub("%s+$", "")
    if s == "" then
      return 0
    end
    local neg = false
    if string.sub(s, 1, 1) == "-" then
      neg = true
      s = string.sub(s, 2)
    elseif string.sub(s, 1, 1) == "+" then
      s = string.sub(s, 2)
    end
    if not s:match("^%d+$") then
      raiseDecodeError("lark frame: invalid integer value")
    end
    -- Accumulate modulo 2^64 using native Lua 5.5 64-bit wrap-around arithmetic
    -- so unsigned 64-bit decimal strings up to 2^64-1 round-trip accurately.
    local acc = 0
    for i = 1, #s do
      acc = acc * 10 + (string.byte(s, i) - 48)
    end
    if neg then
      acc = -acc
    end
    return acc
  end
  raiseDecodeError("lark frame: invalid integer value")
end

local function asInt32(val)
  local u32 = val & 0xffffffff
  if u32 >= 0x80000000 then
    return u32 - 0x100000000
  end
  return u32
end

local function pushVarint(out, value)
  local v = toInt64(value)
  -- (v & ~0x7f) ~= 0 works for all 64-bit patterns including unsigned >= 2^63 (negative int64).
  while (v & ~0x7f) ~= 0 do
    out[#out + 1] = string.char((v & 0x7f) | 0x80)
    v = v >> 7
  end
  out[#out + 1] = string.char(v & 0x7f)
end

local function readVarint(bytes, pos)
  local len = #bytes
  local result = 0
  local shift = 0
  while pos <= len do
    local b = string.byte(bytes, pos)
    pos = pos + 1
    if shift < 64 then
      result = result | ((b & 0x7f) << shift)
    end
    if (b & 0x80) == 0 then
      return result, pos
    end
    shift = shift + 7
    if shift > 64 then
      raiseDecodeError("lark frame: varint overflow")
    end
  end
  raiseDecodeError("lark frame: truncated varint")
end

local function readLength(bytes, pos, label)
  local n, nextPos = readVarint(bytes, pos)
  local rem = #bytes - nextPos + 1
  if math.ult(rem, n) then
    raiseDecodeError(label .. ": truncated length-delimited field")
  end
  local endPos = nextPos + n - 1
  if endPos < nextPos - 1 or endPos > #bytes then
    raiseDecodeError(label .. ": field exceeds buffer bounds")
  end
  return string.sub(bytes, nextPos, endPos), endPos + 1
end

local function skipFixed(bytes, pos, width, label)
  if #bytes - pos + 1 < width then
    raiseDecodeError(label .. ": truncated fixed" .. tostring(width * 8) .. " field")
  end
  return pos + width
end

local function pushTag(out, fieldNum, wireType)
  pushVarint(out, (fieldNum << 3) | wireType)
end

local function pushBytesField(out, fieldNum, data)
  local bytes = toBinaryString(data)
  pushTag(out, fieldNum, 2)
  pushVarint(out, #bytes)
  if #bytes > 0 then
    out[#out + 1] = bytes
  end
end

local function pushStringField(out, fieldNum, str)
  if str == nil then
    return
  end
  pushBytesField(out, fieldNum, tostring(str))
end

local function encodeHeader(hdr)
  if type(hdr) ~= "table" or type(hdr.key) ~= "string" or hdr.key == "" then
    raiseDecodeError("lark header: key must be non-empty string")
  end
  if type(hdr.value) ~= "string" then
    raiseDecodeError("lark header: value must be string")
  end
  local out = {}
  pushStringField(out, 1, hdr.key)
  pushStringField(out, 2, hdr.value)
  return table.concat(out)
end

local function decodeHeader(bytes)
  local pos = 1
  local len = #bytes
  local key = ""
  local value = ""
  while pos <= len do
    local tag, nextPos = readVarint(bytes, pos)
    pos = nextPos
    local field = tag >> 3
    local wire = tag & 7
    if wire == 2 then
      local slice
      slice, pos = readLength(bytes, pos, "lark header")
      if field == 1 then
        key = slice
      elseif field == 2 then
        value = slice
      end
    elseif wire == 0 then
      local _
      _, pos = readVarint(bytes, pos)
    elseif wire == 1 then
      pos = skipFixed(bytes, pos, 8, "lark header")
    elseif wire == 5 then
      pos = skipFixed(bytes, pos, 4, "lark header")
    else
      raiseDecodeError("lark header: unsupported wire type " .. tostring(wire))
    end
  end
  if key == "" then
    raiseDecodeError("lark header: key must be non-empty string")
  end
  return { key = key, value = value }
end

local function encodeFrame(frame)
  if type(frame) ~= "table" then
    raiseDecodeError("lark frame: invalid frame object")
  end
  local out = {}
  local seqId = toInt64(frame.seqId)
  if seqId ~= 0 then
    pushTag(out, 1, 0)
    pushVarint(out, seqId)
  end
  local logId = toInt64(frame.logId)
  if logId ~= 0 then
    pushTag(out, 2, 0)
    pushVarint(out, logId)
  end
  local service = toInt64(frame.service)
  if service ~= 0 then
    pushTag(out, 3, 0)
    pushVarint(out, service)
  end
  local method = toInt64(frame.method)
  if method ~= 0 then
    pushTag(out, 4, 0)
    pushVarint(out, method)
  end
  if type(frame.headers) == "table" then
    for _, hdr in ipairs(frame.headers) do
      pushBytesField(out, 5, encodeHeader(hdr))
    end
  end
  if frame.payloadEncoding ~= nil and frame.payloadEncoding ~= "" then
    pushStringField(out, 6, frame.payloadEncoding)
  end
  if frame.payloadType ~= nil and frame.payloadType ~= "" then
    pushStringField(out, 7, frame.payloadType)
  end
  if frame.payload ~= nil then
    local payload = toBinaryString(frame.payload)
    if #payload > 0 then
      pushBytesField(out, 8, payload)
    end
  end
  if frame.logIdNew ~= nil and frame.logIdNew ~= "" then
    pushStringField(out, 9, frame.logIdNew)
  end
  return table.concat(out)
end

local function decodeFrame(raw)
  local bytes = toBinaryString(raw)
  if #bytes == 0 then
    raiseDecodeError("lark frame: payload is empty")
  end
  local frame = {
    seqId = 0,
    logId = 0,
    service = 0,
    method = 0,
    headers = {},
    payloadEncoding = nil,
    payloadType = nil,
    payload = nil,
    logIdNew = nil,
  }
  local pos = 1
  local len = #bytes
  while pos <= len do
    local tag, nextPos = readVarint(bytes, pos)
    pos = nextPos
    local field = tag >> 3
    local wire = tag & 7
    if wire == 0 then
      local val
      val, pos = readVarint(bytes, pos)
      if field == 1 then
        frame.seqId = val
      elseif field == 2 then
        frame.logId = val
      elseif field == 3 then
        frame.service = asInt32(val)
      elseif field == 4 then
        frame.method = asInt32(val)
      end
    elseif wire == 2 then
      local slice
      slice, pos = readLength(bytes, pos, "lark frame")
      if field == 5 then
        frame.headers[#frame.headers + 1] = decodeHeader(slice)
      elseif field == 6 then
        frame.payloadEncoding = slice
      elseif field == 7 then
        frame.payloadType = slice
      elseif field == 8 then
        frame.payload = slice
      elseif field == 9 then
        frame.logIdNew = slice
      end
    elseif wire == 1 then
      pos = skipFixed(bytes, pos, 8, "lark frame")
    elseif wire == 5 then
      pos = skipFixed(bytes, pos, 4, "lark frame")
    else
      raiseDecodeError("lark frame: unsupported wire type " .. tostring(wire))
    end
  end
  return frame
end

local function getHeaderValues(frame, key)
  local out = {}
  if type(frame) == "table" and type(frame.headers) == "table" then
    for _, h in ipairs(frame.headers) do
      if type(h) == "table" and h.key == key then
        out[#out + 1] = h.value
      end
    end
  end
  return out
end

local function getHeader(frame, key)
  if type(frame) == "table" and type(frame.headers) == "table" then
    for _, h in ipairs(frame.headers) do
      if type(h) == "table" and h.key == key then
        return h.value
      end
    end
  end
  return ""
end

return {
  newFrameDecodeError = newFrameDecodeError,
  isFrameDecodeError = isFrameDecodeError,
  toBinaryString = toBinaryString,
  encodeFrame = encodeFrame,
  decodeFrame = decodeFrame,
  getHeaderValues = getHeaderValues,
  getHeader = getHeader,
}
