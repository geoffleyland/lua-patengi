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



function statement:_exec(result_method, ...)
  self._stmt:reset()
  self:_bind(...)
  if self._stmt:step() == sqlite3.ROW then
    return self._stmt[result_method](self._stmt)
  end
end

function statement:exec(...)  return self:_exec("get_values", ...) end
function statement:nexec(...) return self:_exec("get_named_values", ...) end
function statement:uexec(...) return self:_exec("get_uvalues", ...) end



function statement:_rows(result_method, ...)
  self._stmt:reset()
  self:_bind(...)
  return self._stmt[result_method](self._stmt)
end


function statement:rows(...)  return self:_rows("rows", ...) end
function statement:nrows(...) return self:_rows("nrows", ...) end
function statement:urows(...) return self:_rows("urows", ...) end


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
