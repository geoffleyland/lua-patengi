local patengi = require("patengi")

local db_names =
{
  sqlite3 = "sqlite3:test.db",
  pgsql = "pgsql:dbname=test,user=pgsql",
}

describe("front", function()
  local db_name = db_names[os.getenv("DB") or "sqlite3"]

  it("should create a database", function()
      assert.has_no.errors(function() patengi.open(db_name) end)
  end)

end)