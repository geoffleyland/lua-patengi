# lua-patengi - Yet another database adapter for Lua

## 1. What?

P&#257;tengi is a thin layer over other database adapters that makes them look the
same.

If the adapter lacks features, it fakes them.  (For the moment, this only
means that it fakes named parameters for pgsql).

It can't fix the differences in SQL dialects, but it has a cheap macro (in
the C sense) mechanism.

P&#257;tengi is M&#257;ori for "database" (well, storehouse.
P&#257;tengi raraunga would be better but that's a bit long)


## 2. Why?

Mostly because I didn't read the docs for
[lua-dbi](https://code.google.com/p/luadbi/) carefully enough and thought it
didn't support query parameters.

By the time I was kindly corrected, I'd already got this far and my binding
suited me a little better.


## 3. How?

Sorry, I haven't got to makefiles or rockspecs yet.


## 4. Requirements

Lua >= 5.1 or LuaJIT >= 2.0.0.


## 5. Issues

+ Incomplete


## 6. Wishlist

+ Tests?


## 6. Alternatives

+ Many!