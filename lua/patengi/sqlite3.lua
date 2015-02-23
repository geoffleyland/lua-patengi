local sqlite3 = require"lsqlite3"


----------------------------------------------------------------------

local function check_result(result, db, sql)
  if result == nil or (type(result) == "number" and result ~= sqlite3.OK) then
    local message = ("SQL error: %s"):format(db:error_message())
    if sql then
      message = message..("\nwhile executing:\n%s\n"):format(sql:gsub("^", "  "))
    end
    error(message)
  end
  return result
end


local function translate_args(sql)
  local rest = sql
  local newsql = {}
  while true do
    local part, quote
    part, quote, rest = rest:match("([^\"\']*)([\"\']?)(.*)")
    part = part:gsub("[$:](%a[%w_]*)", function(name) return ":"..name end)
    newsql[#newsql+1] = part
    if quote == "" then break end
    newsql[#newsql+1] = quote
    local in_quote
    in_quote, rest = rest:match(("(.-%s)(.*)"):format(quote))
    newsql[#newsql+1] = in_quote
  end

  return table.concat(newsql, "")
end


----------------------------------------------------------------------

local statement = {}
statement.__index = statement


function statement:new(db, sql)
  sql = translate_args(sql)
  local stmt = check_result(db:prepare(sql), db, sql)

  return setmetatable({ _db = db, _sql = sql, _stmt = stmt }, statement)
end


function statement:_bind(arg1, ...)
  local arg_count = select("#", ...)
  if type(arg1) == "table" and arg_count == 0 then
    if arg1[1] then  -- guess they've passed in an "array" of arguments
      self._stmt:bind_values(unpack(arg1))
    else -- guess they've passed in table of named args
      self._stmt:bind_names(arg1)
    end
  elseif arg1 ~= nil or arg_count ~= 0 then
    self._stmt:bind_values(arg1, ...)
  end
end


function statement:exec(...)
  self._stmt:reset()
  self:_bind(...)
  self._stmt:step()
  if self._stmt:columns() > 0 then
    return self._stmt:get_uvalues()
  end
end


function statement:nrows(...)
  self._stmt:reset()
  self:_bind(...)
  return self._stmt:nrows()
end


----------------------------------------------------------------------


local thisdb = {}
thisdb.__index = thisdb


function thisdb:type()
  return "sqlite3"
end


function thisdb:error(sql)
  error(("SQL error: %s\nwhile executing:\n%s\n"):format(self._db:error_message(), sql:gsub("^", "  ")))
end


function thisdb:exec(sql)
  local result = check_result(self._db:exec(sql), self._db, sql)
end


function thisdb:prepare(sql)
  return statement:new(self._db, sql)
end


function thisdb:close()
  self._db:close()
end


return 
{
  open = function(...)
      return setmetatable({ _db=sqlite3.open(...) }, thisdb)
    end,
}
