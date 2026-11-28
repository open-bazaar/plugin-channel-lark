-- Deterministic test harness for real bcn + bcc Lark channel E2E tests.
local process = require("bazaar.process")

local SECTIONS = 80
local sessions = {}

local function stripAttachmentSuffix(text)
  return (text:gsub(" %[%d+ attachments?: .*%]%s*$", ""))
end

local function parseSender(rest)
  local handle, afterWithName = rest:match("^@([^%s%(:]+)%([^%)\n]*%): (.*)$")
  if handle then
    return handle, afterWithName
  end
  local bareHandle, afterBare = rest:match("^@([^%s%(:]+): (.*)$")
  if bareHandle then
    return bareHandle, afterBare
  end
  return "", rest
end

local function runBCC(sess, args, stdinData)
  local fullArgs = { "bcc" }
  for i = 1, #args do
    fullArgs[#fullArgs + 1] = args[i]
  end
  local spec = {
    command = "bcn",
    args = fullArgs,
    cwd = sess.workspace,
    env = sess.env,
    timeoutMs = 30000,
  }
  if stdinData ~= nil then
    spec.stdin = stdinData
  end
  local res = process.exec(spec)
  if res.code ~= 0 then
    error("bcc failed (code=" .. tostring(res.code) .. "): " .. tostring(res.stderr or res.stdout))
  end
  return tostring(res.stdout or "")
end

local function sendBCC(sess, target, body, extraArgs)
  local args = { "message", "send", "--target", target }
  if extraArgs then
    for i = 1, #extraArgs do
      args[#args + 1] = extraArgs[i]
    end
  end
  local out = runBCC(sess, args, body or "")
  if out:find("Your message has been saved as a draft", 1, true) then
    runBCC(sess, { "message", "send", "--send-draft", "--target", target }, nil)
  end
end

local function buildLongBody(marker)
  local sections = { "# " .. marker .. " long" }
  for number = 1, SECTIONS do
    local lines = { string.format("## %s-%04d\n", marker, number) }
    for item = 1, 6 do
      lines[#lines + 1] = string.format(
        "- item %d of section %d belongs to a checklist that needs more than one Lark post",
        item,
        number
      )
    end
    sections[#sections + 1] = table.concat(lines, "\n")
    if number == math.floor(SECTIONS / 2) then
      local fenced = { "```text" }
      for line = 1, 20 do
        fenced[#fenced + 1] = string.format("%s fenced %d", marker, line)
      end
      fenced[#fenced + 1] = "```"
      sections[#sections + 1] = table.concat(fenced, "\n")
    end
  end
  sections[#sections + 1] = string.format("END %s %d", marker, SECTIONS)
  return table.concat(sections, "\n\n")
end

local function processInbox(sess)
  local inbox = runBCC(sess, { "message", "check" }, nil)
  if inbox == "" or inbox:find("No more new messages.", 1, true) then
    return
  end

  local quoted = {}
  local messages = {}

  for rawLine in (inbox .. "\n"):gmatch("(.-)\r?\n") do
    if rawLine ~= "" then
      local refId, refRest = rawLine:match("^%[%d+/%d+ seq=%d+ msg=([0-9a-f%-]+) [^%]]*%] (.*)$")
      if refId then
        local _, body = parseSender(refRest)
        quoted[refId] = stripAttachmentSuffix(body)
      else
        local target, msgId, meta, rest = rawLine:match("^%[target=(.-) msg=([0-9a-f%-]+) ([^%]]*)%] (.*)$")
        if target and msgId then
          local handle, body = parseSender(rest)
          local replyTo = meta:match("reply_to=([0-9a-f%-]+)")
          quoted[msgId] = stripAttachmentSuffix(body)
          messages[#messages + 1] = {
            target = target,
            id = msgId,
            handle = handle,
            body = body,
            replyTo = replyTo,
          }
        end
      end
    end
  end

  for i = 1, #messages do
    local msg = messages[i]
    local marker, action = msg.body:match("(e2e%-[0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f][0-9a-f]) (%a+)")
    if marker and action then
      if action == "reply" then
        sendBCC(sess, msg.target, marker .. " reply", { "--reply-to", msg.id })
      elseif action == "long" then
        sendBCC(sess, msg.target, buildLongBody(marker), nil)
      elseif action == "attach" then
        sendBCC(sess, msg.target, "", { "--attachment", "attachment.txt" })
      elseif action == "quote" then
        local text = msg.replyTo and quoted[msg.replyTo] or nil
        if (not text) and msg.replyTo then
          local hist = runBCC(sess, { "message", "read", "--target", msg.target, "--around", msg.replyTo }, nil)
          for hline in (hist .. "\n"):gmatch("(.-)\r?\n") do
            local hid, hrest = hline:match("^%[%d+/%d+ seq=%d+ msg=([0-9a-f%-]+) [^%]]*%] (.*)$")
            if hid == msg.replyTo then
              local _, hbody = parseSender(hrest)
              text = stripAttachmentSuffix(hbody)
              break
            end
          end
        end
        if not text then
          error("e2e harness: could not resolve quoted message " .. tostring(msg.replyTo))
        end
        sendBCC(sess, msg.target, text, nil)
      elseif action == "echo" then
        local attPath = msg.body:match("path:([^,%)%]]+)")
        if not attPath or attPath == "" then
          error("e2e harness: echo command arrived without materialized attachment path: " .. msg.body)
        end
        sendBCC(sess, msg.target, "", { "--attachment", attPath })
      end
    end
  end
end

local function inspect(_ctx, _config)
  return {
    available = true,
    version = "0.4.0-e2e",
    models = { { id = "e2e-echo", name = "E2E Echo", default = true } },
    skills = {},
    mcpServers = {},
  }
end

local function openSession(_ctx, _config, input, emit)
  local inp = type(input) == "table" and input or {}
  local sid = tostring(inp.sessionId or "e2e-session")
  sessions[sid] = {
    id = sid,
    env = type(inp.env) == "table" and inp.env or {},
    workspace = tostring(inp.workspace or ""),
    emit = emit,
  }
  emit({
    type = "session_started",
    sessionId = sid,
    providerSessionId = sid,
    model = "e2e-echo",
  })
  return {
    sessionId = sid,
    providerSessionId = sid,
    model = "e2e-echo",
  }
end

local function startTurn(sessionId, input)
  local sess = sessions[sessionId]
  if not sess then
    error("e2e harness: unknown session " .. tostring(sessionId))
  end
  local turnId = tostring((type(input) == "table" and input.turnId) or "turn")
  sess.emit({ type = "turn_started", sessionId = sessionId, turnId = turnId })
  local ok, err = pcall(processInbox, sess)
  if not ok then
    sess.emit({
      type = "turn_failed",
      sessionId = sessionId,
      turnId = turnId,
      error = tostring(err),
      failureReason = "execution",
    })
    error(err)
  end
  sess.emit({ type = "turn_completed", sessionId = sessionId, turnId = turnId })
end

local function cancelTurn(_sessionId, _turnId)
  return true
end

local function closeSession(sessionId)
  sessions[sessionId] = nil
end

return {
  inspect = inspect,
  openSession = openSession,
  startTurn = startTurn,
  cancelTurn = cancelTurn,
  closeSession = closeSession,
}
