local sqlite3 = require"lsqlite3"
local pttranslate = require("patengi.translations").translate


----------------------------------------------------------------------

local OK_RESULTS =
{
  [sqlite3.OK]      = true,
  [sqlite3.ROW]     = true,
  [sqlite3.DONE]    = true,
}

local function check_result(result, db, sql)
  if result == nil or (type(result) == "number" and not OK_RESULTS[result]) then
    local message = ("SQL error %s: %s"):format(tostring(result), db:error_message())
    if sql then
      message = message..("\nwhile executing:\n%s\n"):format(sql:gsub("^", "  "))
    end
    error(message)
  end
  return result
end


local function translate(sql)
  local rest = pttranslate(sql, "sqlite3")
  local newsql = {}
  while true do
    local part, quote
    part, quote, rest = rest:match("([^\"\']*)([\"\']?)(.*)")
    part = part:gsub("[$:]([%w_]+)", function(name) return ":"..name end)
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
  sql = translate(sql)
  local stmt = check_result(db:prepare(sql), db, sql)

  return setmetatable({ _db = db, _sql = sql, _stmt = stmt }, statement)
end


function statement:check(result)
  return check_result(result, self._db, self._sql)
end


function statement:_bind(arg1, ...)
  local arg_count = select("#", ...)
  if type(arg1) == "table" and arg_count == 0 then
    if arg1[1] then  -- guess they've passed in an "array" of arguments
      self:check(self._stmt:bind_values(unpack(arg1)))
    else -- guess they've passed in table of named args
      self:check(self._stmt:bind_names(arg1))
    end
  elseif arg1 ~= nil or arg_count ~= 0 then
    self:check(self._stmt:bind_values(arg1, ...))
  end
end


function statement:_exec(result_method, ...)
  self:check(self._stmt:reset())
  self:_bind(...)
  if self:check(self._stmt:step()) == sqlite3.ROW then
    return self._stmt[result_method](self._stmt)
  end
end

function statement:exec(...)  return self:_exec("get_values", ...) end
function statement:nexec(...) return self:_exec("get_named_values", ...) end
function statement:uexec(...) return self:_exec("get_uvalues", ...) end



function statement:_rows(result_method, ...)
  self:check(self._stmt:reset())
  self:_bind(...)
  return self._stmt[result_method](self._stmt)
end

function statement:rows(...)  return self:_rows("rows", ...) end
function statement:nrows(...) return self:_rows("nrows", ...) end
function statement:urows(...) return self:_rows("urows", ...) end


------------------------------------------------------------------------------

local thisdb = {}
thisdb.__index = thisdb


function thisdb:type()
  return "sqlite3"
end


function thisdb:close()
  self._db:close()
end


function thisdb:last_insert_id()
  return self:uexec("SELECT last_insert_rowid();")
end


function thisdb:error(sql)
  error(("SQL error: %s\nwhile executing:\n%s\n"):format(self._db:error_message(), sql:gsub("^", "  ")))
end


function thisdb:prepare(sql)
  return statement:new(self._db, sql)
end


function thisdb:exec(sql, ...)  return self:prepare(sql):exec(...) end
function thisdb:nexec(sql, ...) return self:prepare(sql):nexec(...) end
function thisdb:uexec(sql, ...) return self:prepare(sql):uexec(...) end


function thisdb:_rows(sql, result_method, arg1, ...)
  if arg1 then
    local S = self:prepare(sql)
    return S[result_method](S, arg1, ...)
  else
    return self._db[result_method](self._db, sql)
  end
end

function thisdb:rows(sql, ...)  return self:_rows(sql, "rows", ...) end
function thisdb:nrows(sql, ...) return self:_rows(sql, "nrows", ...) end
function thisdb:urows(sql, ...) return self:_rows(sql, "urows", ...) end


------------------------------------------------------------------------------

return
{
  open = function(...)
      return setmetatable({ _db=assert(sqlite3.open(...))}, thisdb)
    end,
}

------------------------------------------------------------------------------
