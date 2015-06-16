local pgsql = require"pgsql"
local pttranslate = require("patengi.translations").translate


------------------------------------------------------------------------------

local OK_STATUS =
{
  [pgsql.CONNECTION_OK]     = true,
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


local function translate(sql)
  local map
  local rest = pttranslate(sql, "pgsql")
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

------------------------------------------------------------------------------


local function array_rows(result, row)
  if row > result:ntuples() then return end

  local t = {}
  for i = 1, result:nfields() do
    t[i] = get_value(result, row, i)
  end
  return t
end


local function return_rows(result, row)
  local r = array_rows(result, row)
  if r then return unpack(r) end
end


local function name_rows(result, row)
  if row > result:ntuples() then return end

  local t = {}
  for i = 1, result:nfields() do
    t[result:fname(i)] = get_value(result, row, i)
  end
  return t
end


local function row_iterator(result, result_fn, sql)
  check_result(result, sql)
  local i = 0
  local lim = result:ntuples()

  return function()
    i = i + 1
    if i <= lim then
      return result_fn(result, i)
    end
  end
end


------------------------------------------------------------------------------

local statement = {}
statement.__index = statement

local statement_count = 0
function statement:new(db, sql)
  local map
  sql, map = translate(sql)

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


function statement:__exec(result_fn, ...)
  self:_prep(...)
  return result_fn(
    check_result(self._db:execPrepared(self._name, ...), self._sql), 1)
end

function statement:_exec(result_fn, ...)
  return self:__exec(result_fn, marshall_args(self._map, ...))
end

function statement:exec(...)  return self:_exec(array_rows, ...) end
function statement:nexec(...) return self:_exec(name_rows, ...) end
function statement:uexec(...) return self:_exec(return_rows, ...) end


function statement:__rows(result_fn, ...)
  self:_prep(...)
  return row_iterator(self._db:execPrepared(self._name, ...), result_fn, self._sql)
end

function statement:_rows(result_fn, ...)
  return self:__rows(result_fn, marshall_args(self._map, ...))
end

function statement:rows(...)  return self:_rows(array_rows, ...) end
function statement:nrows(...) return self:_rows(name_rows, ...) end
function statement:urows(...) return self:_rows(return_rows, ...) end


------------------------------------------------------------------------------

local thisdb = {}
thisdb.__index = thisdb


function thisdb:type()
  return "pgsql"
end


function thisdb:prepare(sql)
  return statement:new(self._db, sql)
end


function thisdb:__exec(sql, result_fn, arg1, ...)
  if arg1 then
    return result_fn(check_result(self._db:execParams(sql, arg1, ...), sql), 1)
  else
    return result_fn(check_result(self._db:exec(sql), sql), 1)
  end
end

function thisdb:_exec(sql, result_fn, ...)
  local tsql, map = translate(sql)
  return self:__exec(tsql, result_fn, marshall_args(map, ...))
end

function thisdb:exec(sql, ...)  return self:_exec(sql, array_rows, ...) end
function thisdb:nexec(sql, ...) return self:_exec(sql, name_rows, ...) end
function thisdb:uexec(sql, ...) return self:_exec(sql, return_rows, ...) end


function thisdb:__rows(sql, result_fn, arg1, ...)
  if arg1 then
    return row_iterator(self._db:execParams(sql, arg1, ...), result_fn, sql)
  else
    return row_iterator(self._db:exec(sql), result_fn, sql)
  end
end

function thisdb:_rows(sql, result_fn, ...)
  local tsql, map = translate(sql)
  return self:__rows(tsql, result_fn, marshall_args(map, ...))
end

function thisdb:rows(sql, ...)  return self:_rows(sql, array_rows, ...) end
function thisdb:nrows(sql, ...) return self:_rows(sql, name_rows, ...) end
function thisdb:urows(sql, ...) return self:_rows(sql, return_rows, ...) end


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

