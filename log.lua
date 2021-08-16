local policies = require "kong.plugins.response-datalimiting.policies"

local ngx = ngx
local kong = kong
local _M = {}


local function log(premature, conf, limits, identifier, current_timestamp, cost)
  if premature then
    return
  end

  -- 请求通过后，增加设置的 day/month/year/total 的字段值
  policies[conf.policy].increment(conf, limits, identifier, current_timestamp, cost)
end


function _M.execute(conf, limits, identifier, current_timestamp, cost)
  local ok, err = ngx.timer.at(0, log, conf, limits, identifier, current_timestamp, cost)
  if not ok then
    kong.log.err("failed to create timer: ", err)
  end
end

return _M
