local policy_cluster = require "kong.plugins.response-datalimiting.policies.cluster"
local EXPIRATION = require "kong.plugins.response-datalimiting.expiration"
local reports = require "kong.reports"
local redis = require "resty.redis"

local ngx = ngx
local kong = kong
local null = ngx.null
local pairs = pairs
local fmt = string.format

local EMPTY_UUID = "00000000-0000-0000-0000-000000000000"

local function is_present(str)
  return str and str ~= "" and str ~= null
end

local function get_service_and_route_ids(conf)
  conf = conf or {}

  local service_id = conf.service_id
  if not service_id or service_id == null then
    service_id = EMPTY_UUID
  end

  local route_id = conf.route_id
  if not route_id or route_id == null then
    route_id = EMPTY_UUID
  end

  return service_id, route_id
end

local get_local_key = function(conf, identifier, period, period_date)
  local service_id, route_id = get_service_and_route_ids(conf)
  return fmt("response-datalimit:%s:%s:%s:%s:%s", route_id, service_id, identifier, period_date, period)
end

local sock_opts = {}

local function get_redis_connection(conf)
  local red = redis:new()
  red:set_timeout(conf.redis_timeout)
  sock_opts.pool = conf.redis_database and conf.redis_host .. ":" .. conf.redis_port .. ":" .. conf.redis_database
  local ok, err = red:connect(conf.redis_host, conf.redis_port, sock_opts)
  if not ok then
    kong.log.err("failed to connect to Redis: ", err)
    return nil, err
  end

  local times, err = red:get_reused_times()
  if err then
    kong.log.err("failed to get connect reused times: ", err)
    return nil, err
  end

  if times == 0 then
    if is_present(conf.redis_password) then
      local ok, err = red:auth(conf.redis_password)
      if not ok then
        kong.log.err("failed to auth Redis: ", err)
        return nil, err
      end
    end

    if conf.redis_database ~= 0 then
      local ok, err = red:select(conf.redis_database)
      if not ok then
        kong.log.err("failed to change Redis database: ", err)
        return nil, err
      end
    end
  end

  return red
end

local function format_period_date(now)
  return {
    day = os.date("%Y-%m-%d", now),
    month = os.date("%Y-%m", now),
    year = os.date("%Y", now),
    total = math.ceil(os.date("%Y", now) + 10)
  }
end

return {
  ["cluster"] = {
    increment = function(conf, limits, identifier, current_timestamp, value)
      local db = kong.db
      local service_id, route_id = get_service_and_route_ids(conf)
      local policy = policy_cluster[db.strategy]
      local ok, err = policy.increment(db.connector, limits, identifier, current_timestamp, service_id, route_id, value)

      if not ok then
        kong.log.err("cluster policy: could not increment ", db.strategy, " counter: ", err)
      end

      return ok, err
    end,
    usage = function(conf, identifier, period, current_timestamp)
      local db = kong.db
      local service_id, route_id = get_service_and_route_ids(conf)
      local policy = policy_cluster[db.strategy]
      local row, err = policy.find(identifier, period, current_timestamp, service_id, route_id)
      if err then
        ngx.log(ngx.ERR, "cluster-get-err >> ", err)
        return nil, err
      end

      if row and row.value ~= null and row.value > 0 then
        return row.value
      end

      return 0
    end
  },
  ["redis"] = {
    increment = function(conf, limits, identifier, current_timestamp, value)
      local red, err = get_redis_connection(conf)
      if not red then
        return nil, err
      end

      local periods = format_period_date(current_timestamp)
      red:init_pipeline()
      for period, period_date in pairs(periods) do
        if limits[period] then
          local cache_key = get_local_key(conf, identifier, period, period_date)
          red:eval([[
            local key, value, expiration = KEYS[1], tonumber(ARGV[1]), ARGV[2]

            if redis.call("incrby", key, value) == value then
              redis.call("expire", key, expiration)
            end
          ]], 1, cache_key, value, EXPIRATION[period])
        end
      end
      local _, err = red:commit_pipeline()

      if err then
        kong.log.err("failed to commit increment pipeline in Redis: ", err)
        return nil, err
      end

      local ok, err = red:set_keepalive(10000, 100)
      if not ok then
        kong.log.err("failed to set Redis keepalive: ", err)
        return nil, err
      end

      return true
    end,
    usage = function(conf, identifier, period, current_timestamp)
      local red, err = get_redis_connection(conf)
      if not red then
        return nil, err
      end

      reports.retrieve_redis_version(red)
      local periods = format_period_date(current_timestamp)
      local cache_key = get_local_key(conf, identifier, period, periods[period])
      local current_metric, err = red:get(cache_key)
      if err then
        ngx.log(ngx.ERR, "redis-get-err >> ", err)
        return nil, err
      end

      if current_metric == null then
        ngx.log(ngx.ERR, "redis-get-current_metric nil")
        current_metric = nil
      end

      -- 第一个参数：max_idle_timeout，表示超时断开时间
      -- 第二个参数：pool_size，表示连接池大小
      local ok, err = red:set_keepalive(10000, 100)
      if not ok then
        ngx.log(ngx.ERR, "failed to set Redis keepalive: ", err)
        kong.log.err("failed to set Redis keepalive: ", err)
      end

      return current_metric or 0
    end
  }
}
