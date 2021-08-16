local body_filter = require "kong.plugins.response-datalimiting.body_filter"
local access = require "kong.plugins.response-datalimiting.access"
local log = require "kong.plugins.response-datalimiting.log"

local ngx = ngx
local kong = kong
local find = string.find
local lower = string.lower
local concat = table.concat

local function is_json_body(content_type)
  return content_type and find(lower(content_type), "application/json", nil, true)
end

-- 定义变量，PRIORITY在一定范围（插件的执行顺序，该值越大，对应的插件执行时期越靠前）内可以随意设立
local ResponseDataLimitingHandler = {
  PRIORITY = 850,
  VERSION = "1.0.0"
}

-- 判断数据总数是否已超限
function ResponseDataLimitingHandler:access(conf)
  access.execute(conf)
end

-- 解析响应数据，对数据总数进行累加
function ResponseDataLimitingHandler:body_filter(conf)
  if #conf.data_list_fields > 0 and is_json_body(kong.response.get_header("Content-Type")) then
    local ctx = ngx.ctx
    local chunk, eof = ngx.arg[1], ngx.arg[2]

    ctx.rt_body_chunks = ctx.rt_body_chunks or {}
    ctx.rt_body_chunk_number = ctx.rt_body_chunk_number or 1

    if eof then
      local chunks = concat(ctx.rt_body_chunks)
      local body = body_filter.execute(conf, chunks)
      ngx.arg[1] = body or chunks

    else
      ctx.rt_body_chunks[ctx.rt_body_chunk_number] = chunk
      ctx.rt_body_chunk_number = ctx.rt_body_chunk_number + 1
      ngx.arg[1] = nil
    end
  end
end

-- 在 header_filter()与body_filter()中无法与redis/postgres等数据库建立连接，
-- 因此通过在log()方法中使用ngx.timer.at设定定时任务来触发数据量的更新
function ResponseDataLimitingHandler:log(conf)
  local ctx = kong.ctx.plugin
  if ctx.rdl_usage then
    log.execute(conf, ctx.rdl_limits, ctx.rdl_identifier, ctx.rdl_current_timestamp, ctx.rdl_cost)
  end
end

return ResponseDataLimitingHandler
