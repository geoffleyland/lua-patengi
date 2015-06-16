local TRANSLATIONS =
{
  ["INTEGER PRIMARY KEY"] =
  {
    sqlite3   = "INTEGER PRIMARY KEY",
    pgsql     = "SERIAL PRIMARY KEY",
  },
  ["SERIAL PRIMARY KEY"] =
  {
    sqlite3   = "INTEGER PRIMARY KEY",
    pgsql     = "SERIAL PRIMARY KEY",
  }
}


local function add_translation(db, text, translation)
  TRANSLATIONS[text] = TRANSLATIONS[text] or {}
  TRANSLATIONS[text][db] = translation
end


local function translate(sql, dbtype)
  return sql:gsub("%b{}", function(key)
      key = key:sub(2,-2)
      local func, args = key:match("(%a[%w_]*)(%b())")
      if func then
        args = args:sub(2, -2)
        local argtable = {}
        for arg in args:gmatch("([^,]*)") do
          argtable[#argtable+1] = arg
        end
        local replacement = TRANSLATIONS[func][dbtype]
        return replacement:gsub("$(%d+)", function(i) return argtable[tonumber(i)] end)
      else
        return TRANSLATIONS[key][dbtype]
      end
    end)
end


return
{
  add_translation = add_translation,
  translate = translate,
}
