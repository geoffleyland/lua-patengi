package = "patengi"
version = "scm-1"
source =
{
  url = "git://github.com/geoffleyland/lua-patengi.git",
  branch = "master",
}
description =
{
  summary = "Yet another database connectivity tool",
  homepage = "http://github.com/geoffleyland/lua-patengi",
  license = "MIT/X11",
  maintainer = "Geoff Leyland <geoff.leyland@incremental.co.nz>"
}
dependencies = { "lua >= 5.1" }
build =
{
  type = "builtin",
  modules =
  {
    patengi = "lua/patengi.lua",
    ["patengi.sqlite3"] = "lua/patengi/sqlite3.lua",
    ["patengi.pgsql"] = "lua/patengi/pgsql.lua",
  },
}
