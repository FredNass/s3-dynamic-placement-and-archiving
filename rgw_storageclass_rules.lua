-- rgw_storageclass_rules.lua (select most restrictive matching rule)
-- Context: preRequest
-- Config file: /etc/ceph/rgw_storageclass_rules.conf

local CONFIG_PATH = "/etc/ceph/rgw_storageclass_rules.conf"

local function log(msg) RGWDebugLog(msg) end

local function trim(s) return (s or ""):gsub("^%s+", ""):gsub("%s+$", "") end

local function to_bool(v, default)
  if not v or v == "" then return default end
  v = string.lower(v)
  if v == "1" or v == "true" or v == "yes" or v == "y" or v == "on" or v == "force" then return true end
  if v == "0" or v == "false" or v == "no" or v == "n" or v == "off" then return false end
  return default
end

-- Size parsing with units (SI and IEC)
-- Accepts: B, K/KB, KiB, M/MB, MiB, G/GB, GiB, T/TB, TiB, P/PB, PiB (case-insensitive)
-- Convention: bare K/M/G/T/P are base 1024 (K=KiB, M=MiB, ...)
local function parse_size(sz)
  sz = trim(sz or "")
  if sz == "" or sz == "*" then return 0, "0B" end
  local num, unit = sz:match("^([%d%.]+)%s*([A-Za-z]*)$")
  if not num then
    num = sz:match("^(%d+)$")
    if num then return tonumber(num), num .. "B" else return 0, "0B" end
  end
  local v = tonumber(num); if not v then return 0, "0B" end
  unit = string.lower(unit or "")
  local base1000 = { ["b"]=1, ["kb"]=1e3, ["mb"]=1e6, ["gb"]=1e9, ["tb"]=1e12, ["pb"]=1e15 }
  local base1024 = {
    [""]=1, ["k"]=1024, ["m"]=1024^2, ["g"]=1024^3, ["t"]=1024^4, ["p"]=1024^5,
    ["kib"]=1024, ["mib"]=1024^2, ["gib"]=1024^3, ["tib"]=1024^4, ["pib"]=1024^5
  }
  local mult = base1000[unit] or base1024[unit]
  if not mult then return math.floor(v), tostring(math.floor(v)) .. "B" end
  local bytes = math.floor(v * mult + 0.5)
  local pretty = num .. unit
  return bytes, pretty
end

-- Rule line: STORAGECLASS;PATTERN;OP;BYTES;BUCKET;TENANT;OVERRIDE
-- OP in {<, <=, =, >=, >, *}
local function parse_rule(line, lineno)
  local parts = {}
  for field in string.gmatch(line, "([^;]+)") do parts[#parts+1] = trim(field) end
  if #parts < 1 then return nil end
  local bytes_num, bytes_pretty = parse_size(parts[4] or "0")
  local r = {
    storage_class = parts[1] or "",
    pattern       = (parts[2] ~= "" and parts[2]) or "*",
    op            = (parts[3] ~= "" and parts[3]) or "*",
    bytes         = bytes_num,
    bytes_str     = bytes_pretty,
    bucket        = (parts[5] ~= "" and parts[5]) or "*",
    tenant        = (parts[6] ~= "" and parts[6]) or "*",
    override      = to_bool(parts[7], false),
    lineno        = lineno
  }
  if r.storage_class == "" then return nil end
  return r
end

local function load_config(path)
  local cfg = {
    mpu_default_class = nil, mpu_force = false,
    default_class = nil, default_force = false,
    rules = {}
  }
  local f = io.open(path, "r")
  if not f then log("Config open failed: " .. path .. " (using empty rules)"); return cfg end
  local lineno = 0
  for line in f:lines() do
    lineno = lineno + 1
    local l = trim(line)
    if l ~= "" and not l:match("^#") then
      local k, v = l:match("^([%w_]+)%s*=%s*(.+)$")
      if k and v then
        k = string.lower(trim(k)); v = trim(v)
        if k == "mpu_default_class" then
          cfg.mpu_default_class = (v ~= "" and v or nil)
        elseif k == "mpu_force" then
          cfg.mpu_force = to_bool(v, false)
        elseif k == "default_class" then
          cfg.default_class = (v ~= "" and v or nil)
        elseif k == "default_force" then
          cfg.default_force = to_bool(v, false)
        else
          local r = parse_rule(l, lineno); if r then table.insert(cfg.rules, r) end
        end
      else
        local r = parse_rule(l, lineno); if r then table.insert(cfg.rules, r) end
      end
    end
  end
  f:close()
  return cfg
end

local function match_pattern(name, pattern)
  if pattern == "*" then return true end
  return (string.find(name or "", pattern) ~= nil)
end

local function size_matches(op, threshold, content_len)
  if op == "*" then return true end
  if not content_len then return false end
  if op == "<"  then return content_len <  threshold end
  if op == "<=" then return content_len <= threshold end
  if op == ">"  then return content_len >  threshold end
  if op == ">=" then return content_len >= threshold end
  if op == "="  then return content_len == threshold end
  return false
end

-- Specificity score for matched rules:
-- +1 if tenant is specific (!="*"), +1 if bucket is specific,
-- +1 if pattern is specific, +1 if op is specific (!="*")
local function specificity_score(r)
  local s = 0
  if r.tenant and r.tenant ~= "*" then s = s + 1 end
  if r.bucket and r.bucket ~= "*" then s = s + 1 end
  if r.pattern and r.pattern ~= "*" then s = s + 1 end
  if r.op and r.op ~= "*" then s = s + 1 end
  return s
end

local function get_http_storage_class()
  return Request.HTTP and Request.HTTP.StorageClass or nil
end

local function set_http_storage_class(sc)
  if Request.HTTP then Request.HTTP.StorageClass = sc end
end

local function is_put_obj()
  return (Request and Request.RGWOp == "put_obj")
end

-- MPU detection without Request.Params
local function detect_mpu_phase()
  if Request and Request.RGWOp then
    local op = Request.RGWOp
    if op == "initiate_multipart" then return "initiate" end
    if op == "upload_part" then return "upload_part" end
    if op == "complete_multipart" then return "complete" end
  end
  local http = Request and Request.HTTP or nil
  local method = http and http.Method or ""
  local qs = http and (http.QueryString or "") or ""
  if method == "POST" and qs:find("uploads", 1, true) then return "initiate" end
  if method == "PUT" and qs:find("partNumber=", 1, true) and qs:find("uploadId=", 1, true) then return "upload_part" end
  if method == "POST" and qs:find("uploadId=", 1, true) then return "complete" end
  return nil
end

if not Request then return end

local cfg = load_config(CONFIG_PATH)

-- MPU: apply default class at initiation if configured
do
  local mpu_phase = detect_mpu_phase()
  if mpu_phase == "initiate" and cfg.mpu_default_class then
    local client_sc = get_http_storage_class()
    if cfg.mpu_force or (not client_sc or client_sc == "") then
      set_http_storage_class(cfg.mpu_default_class)
      log("MPU initiate: apply default StorageClass='" .. cfg.mpu_default_class ..
          "' (force=" .. tostring(cfg.mpu_force) .. ")")
    else
      log("MPU initiate: keep client StorageClass='" .. tostring(client_sc) .. "' (force=false)")
    end
    return
  elseif mpu_phase == "upload_part" or mpu_phase == "complete" then
    return
  end
end

-- Rules for non-MPU PUT only
if not is_put_obj() then return end

local bucket = (Request.Bucket and Request.Bucket.Name) or ""
local tenant = (Request.Bucket and Request.Bucket.Tenant) or ""
local obj    = (Request.Object and Request.Object.Name) or ""
local clen   = Request.ContentLength

local client_sc = get_http_storage_class()
local any_client_sc = (client_sc ~= nil and client_sc ~= "")

local matched_total = 0
local best_rule_idx = nil
local best_score = -1
local best_sc = nil
local best_will_override = false

for idx, r in ipairs(cfg.rules) do
  local reasons = {}
  local ok = true

  if r.tenant ~= "*" and r.tenant ~= tenant then ok = false; reasons[#reasons+1] = "tenant mismatch" end
  if ok and r.bucket ~= "*" and r.bucket ~= bucket then ok = false; reasons[#reasons+1] = "bucket mismatch" end
  if ok and not match_pattern(obj, r.pattern) then ok = false; reasons[#reasons+1] = "name pattern mismatch" end
  if ok and not size_matches(r.op, r.bytes, clen) then ok = false; reasons[#reasons+1] = "size op mismatch" end

  if ok then
    matched_total = matched_total + 1
    local curr_client_sc = get_http_storage_class()
    local will_override = r.override or (not curr_client_sc or curr_client_sc == "")
    local score = specificity_score(r)
    log(string.format(
      "Rule #%d (line %d) MATCH (score=%d) -> %s (override=%s) obj='%s' size=%s bucket='%s' tenant='%s' threshold=%s",
      idx, r.lineno or -1, score, will_override and "candidate" or "not candidate (client SC present)",
      tostring(r.override), obj, tostring(clen), bucket, tenant, r.bytes_str))

    if will_override then
      if score > best_score or (score == best_score and idx > (best_rule_idx or -1)) then
        best_rule_idx = idx
        best_score = score
        best_sc = r.storage_class
        best_will_override = true
      end
    end
  else
    log(string.format(
      "Rule #%d (line %d) NO MATCH: obj='%s' size=%s bucket='%s' tenant='%s' reason=%s threshold=%s",
      idx, r.lineno or -1, obj, tostring(clen), bucket, tenant, table.concat(reasons, ","), r.bytes_str))
  end
end

if matched_total > 1 then
  if best_rule_idx then
    log(string.format("Multiple rules matched (%d); selected most restrictive rule #%d (score=%d)", matched_total, best_rule_idx, best_score))
  else
    local msg = "Multiple rules matched (%d); client StorageClass kept (no eligible override)"
    if not any_client_sc then
      msg = "Multiple rules matched (%d); no eligible override and no client SC -> will try default_class"
    end
    log(string.format(msg, matched_total))
  end
end

-- Apply the most restrictive eligible rule if available
if best_rule_idx and best_sc and best_will_override then
  set_http_storage_class(best_sc)
  log(string.format("Applied StorageClass='%s' by most restrictive rule #%d (score=%d) to object '%s'",
      best_sc, best_rule_idx, best_score, obj))
else
  -- Otherwise, apply default_class if configured
  if cfg.default_class then
    local csc = get_http_storage_class()
    if cfg.default_force or (not csc or csc == "") then
      set_http_storage_class(cfg.default_class)
      log("No candidate rule applied: apply default StorageClass='" .. cfg.default_class ..
          "' (default_force=" .. tostring(cfg.default_force) .. ")")
    else
      log("No candidate rule applied: keep client StorageClass='" .. tostring(csc) .. "' (default_force=false)")
    end
  end
end
