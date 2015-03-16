local patengi = require("patengi")

local db_names =
{
  sqlite3 = "sqlite3:test.db",
  pgsql = "pgsql:dbname=test user=postgres",
}

describe("Database tests", function()
  local db_name = db_names[os.getenv("DB") or "sqlite3"]

  it("should create a database", function()
      assert.has_no.errors(function() patengi.open(db_name) end)
  end)

  it("should do some more tests", function()
      local db
      assert.has_no.errors(function() db = patengi.open(db_name) end)
      assert.has_no.errors(function() db:exec("CREATE TABLE test (name VARCHAR, number INTEGER);") end)
      local s1
      assert.has_no_errors(function() s1 = db:prepare("INSERT INTO test (name, number) VALUES (:name, :number)") end)
      assert.has_no_errors(function()
          for i = 1, 100 do
            s1:exec("number "..tostring(i), i)
          end
        end)
  end)

end)