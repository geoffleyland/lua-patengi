local patengi = require("patengi")

local db_names =
{
  sqlite3 = "sqlite3:test.db",
  pgsql = "pgsql:dbname=test user=geoff",
}

describe("Database tests", function()
  local db_name = db_names[os.getenv("DB") or "sqlite3"]

  it("should create a database", function()
      assert.has_no.errors(function() patengi.open(db_name) end)
  end)

  it("should do some more tests", function()
      local db
      assert.has_no.errors(function() db = patengi.open(db_name) end)
      assert.is.truthy(db)
      assert.has_no.errors(function() db:exec("CREATE TABLE test (name VARCHAR, number INTEGER);") end)
      local s1
      assert.has_no_errors(function() s1 = db:prepare("INSERT INTO test (name, number) VALUES (:name, :number)") end)
      assert.has_no_errors(function()
          for i = 1, 100 do
            s1:exec("number "..tostring(i), i)
          end
        end)

      local SQL = "SELECT name, number FROM test WHERE number = :number"
      for i = 1, 100 do
        local name, number = db:uexec(SQL, i)
        assert.are.equal(name, "number "..tostring(i))
        assert.are.equal(number, i)
        local name, number = db:uexec(SQL, {number=i})
        assert.are.equal(name, "number "..tostring(i))
        assert.are.equal(number, i)
        local t = db:nexec(SQL, i)
        assert.are.equal(t.name, "number "..tostring(i))
        assert.are.equal(t.number, i)
        local a = db:exec(SQL, i)
        assert.are.equal(a[1], "number "..tostring(i))
        assert.are.equal(a[2], i)
      end

      local s2
      assert.has_no_errors(function() s2 = db:prepare(SQL) end)
      for i = 1, 100 do
        local name, number = s2:uexec(i)
        assert.are.equal(name, "number "..tostring(i))
        assert.are.equal(number, i)
        local name, number = s2:uexec{number=i}
        assert.are.equal(name, "number "..tostring(i))
        assert.are.equal(number, i)
        local t = s2:nexec(i)
        assert.are.equal(t.name, "number "..tostring(i))
        assert.are.equal(t.number, i)
        local a = s2:exec(i)
        assert.are.equal(a[1], "number "..tostring(i))
        assert.are.equal(a[2], i)
      end
      assert.has_no_errors(function() s2:exec(101) end)
      assert.has_no_errors(function() s2:nexec(101) end)
      assert.has_no_errors(function() s2:uexec(101) end)
  end)
end)
