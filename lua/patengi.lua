return
{
  open = function(arg1, ...)
      local db, a1 = arg1:match("(%w+):?(.*)")
      if a1 == "" then
        return require("patengi."..db).open(...)
      else
        return require("patengi."..db).open(a1, ...)
      end
    end
}