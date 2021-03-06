return
{
  open = function(arg1, ...)
      local db_module
      -- Choose the database based on the DB connection URL
      if arg1:match("postgres:") or arg1:match("dbname=") then
        db_module = require("patengi.pgsql")
      else
        db_module = require("patengi.sqlite3")
      end
      return db_module.open(arg1, ...)
    end
}
