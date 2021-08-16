local policies = require "kong.plugins.response-datalimiting.policies"

local ngx = ngx
local kong = kong
local pairs = pairs
local error = error
local tostring = tostring


local EMPTY = {}
local HTTP_DATA_TOTAL_EXCEEDED = 429

local _M = {}

local function get_identifier(conf)
  local identifier
  if conf.limit_by == "consumer" then
    identifier = (kong.client.get_consumer() or kong.client.get_credential() or EMPTY).id
  elseif conf.limit_by == "credential" then
    identifier = (kong.client.get_credential() or EMPTY).id
  end

  return identifier or kong.client.get_forwarded_ip()
end

local function get_usage(conf, limits, identifier, current_timestamp)
  local stop
  local usage = {}
  for period, limit in pairs(limits) do
    local current_usage, err = policies[conf.policy].usage(conf, identifier, period, current_timestamp)
    if err then
      return nil, nil
    end

    local remaining = limit - current_usage
    usage[period] = {
      limit = limit,
      remaining = remaining,
    }

    if remaining <= 0 then
      stop = period
    end
  end

  return usage, stop, nil
end

-- 判断已用数据量是否达到阈值
function _M.execute(conf)
  local limits = {
    day = conf.day,
    month = conf.month,
    year = conf.year,
    total = conf.total
  }
  kong.ctx.plugin.rdl_limits = limits
  local current_timestamp = os.time()
  kong.ctx.plugin.rdl_current_timestamp = current_timestamp
  local identifier = get_identifier(conf)
  kong.ctx.plugin.rdl_identifier = identifier

  local usage, stop, err = get_usage(conf, limits, identifier, current_timestamp)
  if err then
    if not conf.fault_tolerant then
      return error(err)
    end
    ngx.log(ngx.ERR, "failed to get usage: ", tostring(err))
  end

  if kong.ctx.plugin.rdl_usage then
    usage = kong.ctx.plugin.rdl_usage
  end

  if stop then
    ngx.log(ngx.ERR, "超限了！stop: ", stop)
    return kong.response.exit(HTTP_DATA_TOTAL_EXCEEDED, "{ \"msg\": API data limit exceeded }")
  end
  kong.ctx.plugin.rdl_usage = usage
end

return _M
