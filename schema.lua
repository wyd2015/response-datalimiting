local typedefs = require "kong.db.schema.typedefs"

local ORDERED_PERIODS = { "day", "month", "year", "total" }

local function validate_periods_order(config)
  for i, lower_period in ipairs(ORDERED_PERIODS) do
    local v1 = config[lower_period]
    if type(v1) == "number" then
      for j = i + 1, #ORDERED_PERIODS do
        local upper_period = ORDERED_PERIODS[j]
        local v2 = config[upper_period]
        if type(v2) == "number" and v2 < v1 then
          return nil, string.format("The limit for %s(%.1f) cannot be lower than the limit for %s(%.1f)",
            upper_period, v2, lower_period, v1)
        end
      end
    end
  end

  return true
end


local function is_dbless()
  local _, database, role = pcall(function()
    return kong.configuration.database,
    kong.configuration.role
  end)

  return database == "off" or role == "control_plane"
end


local policy
if is_dbless() then
  policy = { type = "string", default = "redis", one_of = { "redis", }, }
else
  policy = { type = "string", default = "redis", one_of = { "cluster", "redis", }, }
end

return {
  name = "response-datalimiting",
  fields = {
    { protocols = typedefs.protocols_http },
    { config = {
      type = "record",
      fields = {
        { day = { type = "number", gt = 0 }, },
        { month = { type = "number", gt = 0 }, },
        { year = { type = "number", gt = 0 }, },
        { total = { type = "number", gt = 0 }, },
        { data_list_fields = { type = "string", required = true, default = "data" }},
        { limit_by = { type = "string", default = "consumer", one_of = { "consumer", "credential", "ip" }, }, },
        { policy = policy },
        { fault_tolerant = { type = "boolean", required = true, default = true }, },
        { redis_host = typedefs.host },
        { redis_port = typedefs.port({ default = 6379 }), },
        { redis_password = { type = "string", len_min = 0 }, },
        { redis_timeout = { type = "number", default = 2000 }, },
        { redis_database = { type = "number", default = 0 }, },
        { block_on_first_violation = { type = "boolean", required = true, default = true }, },
      },
      custom_validator = validate_periods_order,
    }},
  },
  entity_checks = {
    { at_least_one_of = { "config.day", "config.month", "config.year", "config.total" } },
    { conditional = {
      if_field = "config.policy", if_match = { eq = "redis" },
      then_field = "config.redis_host", then_match = { required = true },
    } },
    { conditional = {
      if_field = "config.policy", if_match = { eq = "redis" },
      then_field = "config.redis_port", then_match = { required = true },
    } },
    { conditional = {
      if_field = "config.policy", if_match = { eq = "redis" },
      then_field = "config.redis_timeout", then_match = { required = true },
    } },
  },
}
