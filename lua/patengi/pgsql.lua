local pgsql = require"pgsql"


------------------------------------------------------------------------------

local OK_STATUS =
{
  [pgsql.CONNECTION_OK]  = true,
  [pgsql.PGRES_COMMAND_OK]  = true,
  [pgsql.PGRES_TUPLES_OK]   = true,
}

local NUMBER_TYPES =
{
  [20]                      = true, -- INT8OID
  [21]                      = true, -- INT2OID
  [23]                      = true, -- INT4OID
  [700]                     = true, -- FLOAT4OID
  [701]                     = true, -- FLOAT8OID
}


------------------------------------------------------------------------------

local function check_result(result, sql)
  local status = result:status()
  if not OK_STATUS[status] then
    local message = ("SQL error: %s (%s)"):format(
      result:errorMessage(),
      result:resStatus(status)):gsub("\n$", "")
    if sql then
      message = message..("\nwhile executing:\n%s\n"):format(sql:gsub("^", "  "))
    end
    error(message)
  end
  return result
end


local function get_value(result, row, column)
  local v = result:getvalue(row, column)
  if v == "" and result:getisnull(row, column) then
    v = nil
  elseif NUMBER_TYPES[result:ftype(column)] then
    v = tonumber(v)
  end
  return v
end


local function translate_args(sql)
  local map
  local rest = sql
  local newsql = {}
  while true do
    local part, quote
    part, quote, rest = rest:match("([^\"\']*)([\"\']?)(.*)")
    part = part:gsub("[$:](%a[%w_]*)", function(name)
        if name:match("^%d+$") then
          return "$"..name
        else
          map = map or {}
          if map[name] then
            return map[name]
          else
            local n = #map+1
            map[n] = name
            n = "$"..tostring(n)
            map[name] = n
            return n
          end
        end
      end)
    newsql[#newsql+1] = part
    if quote == "" then break end
    newsql[#newsql+1] = quote
    local in_quote
    in_quote, rest = rest:match(("(.-%s)(.*)"):format(quote))
    newsql[#newsql+1] = in_quote
  end

  return table.concat(newsql, ""), map
end


local function marshall_args(map, arg1, ...)
  local arg_count = select("#", ...)
  if type(arg1) == "table" and arg_count == 0 then
    if arg1[1] then  -- guess they've passed in an "array" of arguments
      return unpack(arg[1])
    else -- guess they've passed in table of named args (and
         -- hope they used named args in the query)
      assert(map, "Named arguments passed to a statement that wasn't prepared with named arguments")
      local args = {}
      for i = 1, #map do
        args[i] = arg1[map[i]]
      end
      return unpack(args)
    end
  elseif arg1 ~= nil or arg_count ~= 0 then
    return arg1, ...
  end
end


local function row_to_array_table(result, row)
  local t = {}
  for i = 1, result:nfields() do
    t[i] = get_value(result, row, i)
  end
  return t
end


local function row_to_multiple_return(result, row)
  return unpack(row_to_array_table(result, row))
end


local function row_to_field_table(result, row)
  local t = {}
  for i = 1, result:nfields() do
    t[result:fname(i)] = get_value(result, row, i)
  end
  return t
end


------------------------------------------------------------------------------

local statement = {}
statement.__index = statement

local statement_count = 0
function statement:new(db, sql)
  local map
  sql, map = translate_args(sql)

  statement_count = statement_count + 1
  local name = "statement:"..tostring(statement_count)

  return setmetatable({ _db = db, _sql = sql, _name = name, _map = map }, statement)
end


function statement:_prep(...)
  if not self._prepped then
    local types = {}
    local arg_count = select("#", ...)
    for i = 1, arg_count do
      local v = select(i, ...)
      if type(v) == "number" then
        types[i] = v
      end
    end
    check_result(self._db:prepare(self._name, self._sql, unpack(types, 1, arg_count)), self._sql)
    self._prepped = true
  end
end


function statement:_exec(...)
  self:_prep(...)
  return row_to_multiple_return(
    check_result(self._db:execPrepared(self._name, ...), self._sql), 1)
end


function statement:exec(...)
  return self:_exec(marshall_args(self._map, ...))
end


function statement:_nrows(...)
  self:_prep(...)
  local result = check_result(self._db:execPrepared(self._name, ...), self._sql)

  local i = 0
  local lim = result:ntuples()

  return function()
    i = i + 1
    if i <= lim then
      return row_to_field_table(result, i)
    end
  end
end

  
function statement:nrows(...)
  return self:_nrows(marshall_args(self._map, ...))
end


------------------------------------------------------------------------------

local thisdb = {}
thisdb.__index = thisdb


function thisdb:type()
  return "pgsql"
end


function thisdb:_exec(sql, arg1, ...)
  if arg1 then
    return row_to_multiple_return(
      check_result(self._db:execParams(sql, arg1, ...), sql), 1)
  else
    return row_to_multiple_return(
      check_result(self._db:exec(sql), sql), 1)
  end
end


function thisdb:exec(sql, ...)
  local map
  sql, map = translate_args(sql)

  return self:_exec(sql, marshall_args(map, ...))
end


function thisdb:prepare(sql)
  return statement:new(self._db, sql)
end


function thisdb:close()
  self._db:finish()
end


------------------------------------------------------------------------------

return 
{
  open = function(...)
      local db = pgsql.connectdb(...)
      if db:status() ~= pgsql.CONNECTION_OK then
        error(("Error opening '%s': %s"):format(select(1,...), db:errorMessage()))
      end
      return setmetatable({ _db = db }, thisdb)
    end,
}


------------------------------------------------------------------------------

