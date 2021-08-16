local cjson = require("cjson.safe").new()
local utils = require "kong.tools.utils"

local ngx = ngx
local kong = kong
local pairs = pairs
local find = string.find
local lower = string.lower

cjson.decode_array_with_array_mt(true)

local _M = {}

local function read_json_body(body)
  if body then
    return cjson.decode(body)
  end
end

local function get_array_first_element(array)
  for i, e in pairs(array) do
    if i==1 then
      return e
    end
  end
end

function _M.is_json_body(content_type)
  return content_type and find(lower(content_type), "application/json", nil, true)
end


function _M.execute(conf, buffered_data)
  local data_decoded = read_json_body(buffered_data)
  if data_decoded == nil then
    return
  end

  -- 处理数据集对应的字段，有多层时使用 . 分隔开，如：data.list
  local data_list_fields = conf.data_list_fields
  if not data_list_fields then
    ngx.log(ngx.ERR, "unset data_list_fields for result list")
    return "{ \"msg\": unset data_list_fields for result list }"
  end

  -- 判断返回结果的json字符串中是否设置的首个字段（data.list里的data），如果没有说明设置有误，或者接口异常
  local jsonResp = cjson.encode(data_decoded)
  --ngx.log(ngx.ERR, 'jsonResp is >> ', jsonResp)
  local field_name_array = utils.split(data_list_fields, ".")
  if not string.find(jsonResp, get_array_first_element(field_name_array)) then
    --ngx.log(ngx.ERR, "The set field is not included in the result list")
    return "{ \"msg\": The set field is not included in the result list }"
  end

  -- 解析encode后的json数据，拿到data.list对应的json数组
  for i, name in pairs(field_name_array) do
    local temp = cjson.encode(data_decoded[name])
    ngx.log(ngx.ERR, '[', name, ']-temp is >> ', temp)
    data_decoded = cjson.decode(temp)
  end
  -- 计算数据集长度
  local data_list_length = #data_decoded
  ngx.log(ngx.ERR, 'data list size is: ', data_list_length)

  -- kong.ctx.plugin.* 设置在当前plugin实例范围内生效的变量，以方便在handler的其他方法中使用
  local limits = kong.ctx.plugin.rdl_limits
  local usage, err = kong.ctx.plugin.rdl_usage
  --ngx.log(ngx.ERR, 'RDL-usage: ', cjson.encode(usage))

  if err then
    if not conf.fault_tolerant then
      return error(err)
    end
    ngx.log(ngx.ERR, "failed to get usage: ", tostring(err))
  end

  if usage then
    for period_name, period_value in pairs(limits) do
      if not period_value then
        usage[period_name].limit = 0
        usage[period_name].remaining = 0
      end

      if period_value then
        local current_remaining = usage[period_name].remaining
        local remaining = current_remaining - data_list_length
        usage[period_name].remaining = (remaining > 0 and remaining or 0)
        usage[period_name].limit = period_value
      end
    end

    kong.ctx.plugin.rdl_usage = usage
    kong.ctx.plugin.rdl_cost = data_list_length
    --ngx.log(ngx.ERR, "更新-rdl_usage >>", cjson.encode(kong.ctx.plugin.rdl_usage))
  end
end

return _M
