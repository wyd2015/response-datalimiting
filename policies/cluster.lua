local EXPIRATION = require "kong.plugins.response-datalimiting.expiration"
local cjson = require("cjson.safe").new()
local cassandra = require "cassandra"

local kong = kong
local pairs = pairs
local tostring = tostring
local fmt = string.format
local concat = table.concat

local function format_period_date(now)
  return {
    day = os.date("%Y-%m-%d", now),
    month = os.date("%Y-%m", now),
    year = os.date("%Y", now),
    total = math.ceil(os.date("%Y", now) + 10)
  }
end

local find
do
  local find_pk = {}
  find = function(identifier, period, current_timestamp, service_id, route_id)
    local periods = format_period_date(current_timestamp)
    find_pk.identifier = identifier
    find_pk.period = period
    find_pk.period_date = tostring(periods[period])
    find_pk.service_id = service_id
    find_pk.route_id = route_id
    local query, err = kong.db.response_datalimiting_metrics:select(find_pk)
    --ngx.log(ngx.ERR, "cluster-query >>", cjson.encode(query), ", error >> ", err)
    return query, err
  end
end

return {
  cassandra = {
    increment = function(connector, limits, identifier, current_timestamp, service_id, route_id, value)
      local periods = format_period_date(current_timestamp)
      for period, period_date in pairs(periods) do
        if limits[period] then
          local res, err = connector:query([[
            UPDATE response_datalimiting_metrics SET value = value + ?
            WHERE identifier = ? AND period = ? AND period_date = ? AND service_id = ? AND route_id = ?
          ]], {
            cassandra.counter(value),
            identifier,
            period,
            tostring(period_date),
            cassandra.uuid(service_id),
            cassandra.uuid(route_id),
          })
          if not res then
            kong.log.err("cluster policy: could not increment cassandra counter for period '", period, "': ", err)
          end
        end
      end

      return true
    end,
    find = find,
  },
  postgres = {
    increment = function(connector, limits, identifier, current_timestamp, service_id, route_id, value)
      local buf = { "BEGIN" }
      local len = 1
      local periods = format_period_date(current_timestamp)
      for period, period_date in pairs(periods) do
        if limits[period] then
          len = len + 1
          buf[len] = fmt([[
            INSERT INTO "response_datalimiting_metrics" ("identifier", "period", "period_date", "service_id", "route_id", "value", "ttl")
            VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP AT TIME ZONE 'UTC' + INTERVAL %s)
            ON CONFLICT ("identifier", "period", "period_date", "service_id", "route_id")
            DO UPDATE SET "value" = "response_datalimiting_metrics"."value" + EXCLUDED."value";
          ]],
            connector:escape_literal(identifier),
            connector:escape_literal(period),
            connector:escape_literal(tostring(period_date)),
            connector:escape_literal(service_id),
            connector:escape_literal(route_id),
            connector:escape_literal(value),
            connector:escape_literal(tostring(EXPIRATION[period]) .. " second"))
        end
      end

      if len > 1 then
        local sql
        if len == 2 then
          sql = buf[2]
        else
          buf[len + 1] = "COMMIT;"
          sql = concat(buf, ";\n")
        end

        local res, err = connector:query(sql)
        if not res then
          return nil, err
        end
      end

      return true
    end,
    find = find,
  }
}
