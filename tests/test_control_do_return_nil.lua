local r = require('rethinkdb')
local json = require('json')

status, err = pcall(r.do_, 1, function(x) return nil end)

if status then
  error(err)
else
  print(json.encode(err))
end
