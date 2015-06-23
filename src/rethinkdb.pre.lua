local class = require'reql/class'
local Cursor = require'reql/cursor'
local errors = require'reql/errors'

-- r is both the main export table for the module
-- and a function that wraps a native Lua value in a ReQL datum
local r = {}

function r._logger(err)
  if r.logger then
    r.logger(err)
  elseif type(err) == 'string' then
    error(err)
  elseif type(err) == 'table' and err.msg then
    error(err.msg)
  else
    error('Unknown error type from driver')
  end
end

function r._unb64(data)
  if r.unb64 then
    return r.unb64(data)
  else
    if not r._lib_mime then
      r._lib_mime = require('mime')
    end
    return r._lib_mime.unb64(data)
  end
end

function r._b64(data)
  if r.b64 then
    return r.b64(data)
  else
    if not r._lib_mime then
      r._lib_mime = require('mime')
    end
    return r._lib_mime.b64(data)
  end
end

function r._encode(data)
  if r.encode then
    return r.encode(data)
  elseif r.json_parser then
    return r.json_parser.encode(data)
  else
    if not r._lib_json then
      r._lib_json = require('json')
      r.json_parser = r._lib_json
    end
    return r._lib_json.encode(data)
  end
end

function r._decode(buffer)
  if r.decode then
    return r.decode(buffer)
  elseif r.json_parser then
    return r.json_parser.decode(buffer)
  else
    if not r._lib_json then
      r._lib_json = require('json')
      r.json_parser = r._lib_json
    end
    return r._lib_json.decode(buffer)
  end
end

function r._socket()
  if r.socket then
    return r.socket()
  else
    if not r._lib_socket then
      r._lib_socket = require('socket')
    end
    return r._lib_socket.tcp()
  end
end

local DATUMTERM, ReQLOp
--[[AstNames]]

function r.is_instance(obj, cls, ...)
  if cls == nil then return false end

  if type(cls) == 'string' then
    if type(obj) == cls then
      return true
    end
  elseif type(cls) == 'table' then
    cls = cls.__name
  else
    return false
  end

  if type(obj) == 'table' then
    local obj_cls = obj.__class
    while obj_cls do
      if obj_cls.__name == cls then
        return true
      end
      obj_cls = obj_cls.__parent
    end
  end

  return r.is_instance(obj, ...)
end

setmetatable(r, {
  __call = function(cls, val, nesting_depth)
    if nesting_depth == nil then
      nesting_depth = 20
    end
    if type(nesting_depth) ~= 'number' then
      return r._logger('Second argument to `r(val, nesting_depth)` must be a number.')
    end
    if nesting_depth <= 0 then
      return r._logger('Nesting depth limit exceeded')
    end
    if r.is_instance(val, 'ReQLOp') and type(val.build) == 'function' then
      return val
    end
    if type(val) == 'function' then
      return FUNC({}, val)
    end
    if type(val) == 'table' then
      local array = true
      for k, v in pairs(val) do
        if type(k) ~= 'number' then array = false end
        val[k] = r(v, nesting_depth - 1)
      end
      if array then
        return MAKE_ARRAY({}, unpack(val))
      end
      return MAKE_OBJ(val)
    end
    if type(val) == 'userdata' then
      val = pcall(tostring, val)
      r._logger('Found userdata inserting "' .. val .. '" into query')
      return DATUMTERM(val)
    end
    if type(val) == 'thread' then
      val = pcall(tostring, val)
      r._logger('Cannot insert thread object into query ' .. val)
      return nil
    end
    return DATUMTERM(val)
  end
})

function get_opts(...)
  local args = {...}
  local opt = {}
  local pos_opt = args[#args]
  if (type(pos_opt) == 'table') and (not r.is_instance(pos_opt, 'ReQLOp')) then
    opt = pos_opt
    args[#args] = nil
  end
  return opt, unpack(args)
end

function bytes_to_int(str)
  local t = {str:byte(1,-1)}
  local n = 0
  for k=1,#t do
    n = n + t[k] * 2 ^ ((k - 1) * 8)
  end
  return n
end

function int_to_bytes(num, bytes)
  local res = {}
  local mul = 0
  for k = bytes, 1, -1 do
    local den = 2 ^ (8 * (k - 1))
    res[k] = math.floor(num / den)
    num = math.fmod(num, den)
  end
  return string.char(unpack(res))
end

function convert_pseudotype(obj, opts)
  -- An R_OBJECT may be a regular table or a 'pseudo-type' so we need a
  -- second layer of type switching here on the obfuscated field '$reql_type$'
  local reql_type = obj['$reql_type$']
  if 'TIME' == reql_type then
    local time_format = opts.time_format
    if 'native' == time_format or not time_format then
      if not (obj['epoch_time']) then
        return r._logger(errors.ReQLDriverError('pseudo-type TIME ' .. obj .. ' table missing expected field `epoch_time`.'))
      end

      -- We ignore the timezone field of the pseudo-type TIME table. JS dates do not support timezones.
      -- By converting to a native date table we are intentionally throwing out timezone information.

      -- field 'epoch_time' is in seconds but the Date constructor expects milliseconds
      return obj['epoch_time']
    elseif 'raw' == time_format then
      return obj
    else
      return r._logger(errors.ReQLDriverError('Unknown time_format run option ' .. opts.time_format .. '.'))
    end
  elseif 'GROUPED_DATA' == reql_type then
    local group_format = opts.group_format
    if 'native' == group_format or not group_format then
      -- Don't convert the data into a map, because the keys could be tables which doesn't work in JS
      -- Instead, we have the following format:
      -- [ { 'group': <group>, 'reduction': <value(s)> } }, ... ]
      res = {}
      for i, v in ipairs(obj['data']) do
        res[i] = {
          group = i,
          reduction = v
        }
      end
      obj = res
    elseif 'raw' == group_format then
      return obj
    else
      return r._logger(errors.ReQLDriverError('Unknown group_format run option ' .. opts.group_format .. '.'))
    end
  elseif 'BINARY' == reql_type then
    local binary_format = opts.binary_format
    if 'native' == binary_format or not binary_format then
      if not obj.data then
        return r._logger(errors.ReQLDriverError('pseudo-type BINARY table missing expected field `data`.'))
      end
      return r._unb64(obj.data)
    elseif 'raw' == binary_format then
      return obj
    else
      return r._logger(errors.ReQLDriverError('Unknown binary_format run option ' .. opts.binary_format .. '.'))
    end
  else
    -- Regular table or unknown pseudo type
    return obj
  end
end

function recursively_convert_pseudotype(obj, opts)
  if type(obj) == 'table' then
    for key, value in pairs(obj) do
      obj[key] = recursively_convert_pseudotype(value, opts)
    end
    obj = convert_pseudotype(obj, opts)
  end
  return obj
end

-- All top level exported functions

ast_methods = {
  run = function(self, connection, options, callback)
    -- Valid syntaxes are
    -- connection
    -- connection, callback
    -- connection, options, callback
    -- connection, nil, callback

    -- Handle run(connection, callback)
    if type(options) == 'function' then
      if callback then
        return r._logger('Second argument to `run` cannot be a function if a third argument is provided.')
      end
      callback = options
      options = {}
    end
    -- else we suppose that we have run(connection[, options][, callback])

    if not r.is_instance(connection, 'Connection', 'Pool') then
      if r._pool then
        connection = r._pool
      else
        if callback then
          return callback(errors.ReQLDriverError('First argument to `run` must be a connection.'))
        end
        return r._logger('First argument to `run` must be a connection.')
      end
    end

    return connection:_start(self, callback, options or {})
  end,
  --[[AstMethods]]
}

class_methods = {
  __init = function(self, optargs, ...)
    local args = {...}
    optargs = optargs or {}
    if self.tt == --[[Term.FUNC]] then
      local func = args[1]
      local anon_args = {}
      local arg_nums = {}
      if debug.getinfo then
        local func_info = debug.getinfo(func)
        if func_info.what == 'Lua' and func_info.nparams then
          optargs.arity = func_info.nparams
        end
      end
      for i=1, optargs.arity or 1 do
        table.insert(arg_nums, ReQLOp.next_var_id)
        table.insert(anon_args, VAR({}, ReQLOp.next_var_id))
        ReQLOp.next_var_id = ReQLOp.next_var_id + 1
      end
      func = func(unpack(anon_args))
      if func == nil then
        return r._logger('Anonymous function returned `nil`. Did you forget a `return`?')
      end
      optargs.arity = nil
      args = {arg_nums, func}
    elseif self.tt == --[[Term.BINARY]] then
      local data = args[1]
      if r.is_instance(data, 'ReQLOp') then
      elseif type(data) == 'string' then
        self.base64_data = r._b64(table.remove(args, 1))
      else
        return r._logger('Parameter to `r.binary` must be a string or ReQL query.')
      end
    elseif self.tt == --[[Term.FUNCALL]] then
      local func = table.remove(args)
      if type(func) == 'function' then
        func = FUNC({arity = #args}, func)
      end
      table.insert(args, 1, func)
    elseif self.tt == --[[Term.REDUCE]] then
      args[#args] = FUNC({arity = 2}, args[#args])
    end
    self.args = {}
    self.optargs = {}
    for i, a in ipairs(args) do
      self.args[i] = r(a)
    end
    for k, v in pairs(optargs) do
      self.optargs[k] = r(v)
    end
  end,
  build = function(self)
    if self.tt == --[[Term.BINARY]] and (not self.args[1]) then
      return {
        ['$reql_type$'] = 'BINARY',
        data = self.base64_data
      }
    end
    if self.tt == --[[Term.MAKE_OBJ]] then
      local res = {}
      for key, val in pairs(self.optargs) do
        res[key] = val:build()
      end
      return res
    end
    local args = {}
    for i, arg in ipairs(self.args) do
      args[i] = arg:build()
    end
    res = {self.tt, args}
    if next(self.optargs) then
      local opts = {}
      for key, val in pairs(self.optargs) do
        opts[key] = val:build()
      end
      table.insert(res, opts)
    end
    return res
  end,
  next_var_id = 0,
}

for name, meth in pairs(ast_methods) do
  class_methods[name] = meth
  r[name] = meth
end

-- AST classes

ReQLOp = class('ReQLOp', class_methods)

local meta = {
  __call = function(...)
    return BRACKET({}, ...)
  end,
  __add = function(...)
    return ADD({}, ...)
  end,
  __mul = function(...)
    return MUL({}, ...)
  end,
  __mod = function(...)
    return MOD({}, ...)
  end,
  __sub = function(...)
    return SUB({}, ...)
  end,
  __div = function(...)
    return DIV({}, ...)
  end
}

function ast(name, base)
  for k, v in pairs(meta) do
    base[k] = v
  end
  return class(name, ReQLOp, base)
end

DATUMTERM = ast(
  'DATUMTERM',
  {
    __init = function(self, val)
      if type(val) == 'number' then
        if math.abs(val) == math.huge or val ~= val then
          return r._logger('Illegal non-finite number `' .. val .. '`.')
        end
      end
      self.data = val
    end,
    args = {},
    optargs = {},
    build = function(self)
      if self.data == nil then
        if not r.json_parser then
          r._lib_json = require('json')
          r.json_parser = r._lib_json
        end
        if r.json_parser.null then
          return r.json_parser.null
        end
        if r.json_parser.util then
          return r.json_parser.util.null
        end
      end
      return self.data
    end
  }
)

--[[AstClasses]]

r.connect = class(
  'Connection',
  {
    __init = function(self, host_or_callback, callback)
      local host = {}
      if type(host_or_callback) == 'function' then
        callback = host_or_callback
      elseif type(host_or_callback) == 'string' then
        host = {host = host_or_callback}
      elseif host_or_callback then
        host = host_or_callback
      end
      local cb = function(err, conn)
        if callback then
          local res = callback(err, conn)
          conn:close({noreply_wait = false})
          return res
        end
        return conn, err
      end
      self.weight = 0
      self.host = host.host or self.DEFAULT_HOST
      self.port = host.port or self.DEFAULT_PORT
      self.db = host.db -- left nil if this is not set
      self.auth_key = host.auth_key or self.DEFAULT_AUTH_KEY
      self.timeout = host.timeout or self.DEFAULT_TIMEOUT
      self.outstanding_callbacks = {}
      self.next_token = 1
      self.open = false
      self.buffer = ''
      if self.raw_socket then
        self:close({noreply_wait = false})
      end
      self.raw_socket = r._socket()
      self.raw_socket:settimeout(self.timeout)
      local status, err = self.raw_socket:connect(self.host, self.port)
      if status then
        local buf, err, partial
        -- Initialize connection with magic number to validate version
        self.raw_socket:send(
          '\62\232\117\95' ..
          int_to_bytes(#(self.auth_key), 4) ..
          self.auth_key ..
          '\199\112\105\126'
        )

        -- Now we have to wait for a response from the server
        -- acknowledging the connection
        while 1 do
          buf, err, partial = self.raw_socket:receive(8)
          buf = buf or partial
          if not buf then
            return cb(errors.ReQLDriverError('Server dropped connection with message:  \'' .. status_str .. '\'\n' .. err))
          end
          self.buffer = self.buffer .. buf
          i, j = buf:find('\0')
          if i then
            local status_str = self.buffer:sub(1, i - 1)
            self.buffer = self.buffer:sub(i + 1)
            if status_str == 'SUCCESS' then
              -- We're good, finish setting up the connection
              self.open = true
              return cb(nil, self)
            end
            return cb(errors.ReQLDriverError('Server dropped connection with message: \'' .. status_str .. '\''))
          end
        end
      end
      return cb(errors.ReQLDriverError('Could not connect to ' .. self.host .. ':' .. self.port .. '.\n' .. err))
    end,
    DEFAULT_HOST = 'localhost',
    DEFAULT_PORT = 28015,
    DEFAULT_AUTH_KEY = '',
    DEFAULT_TIMEOUT = 20, -- In seconds
    _get_response = function(self, reqest_token)
      local response_length = 0
      local token = 0
      local buf, err, partial
      -- Buffer data, execute return results if need be
      while true do
        buf, err, partial = self.raw_socket:receive(
          math.max(12, response_length)
        )
        buf = buf or partial
        if (not buf) and err then
          return self:_process_response(
            {
              t = --[[Response.CLIENT_ERROR]],
              r = {'connection returned: ' .. err},
              b = {}
            },
            reqest_token
          )
        end
        self.buffer = self.buffer .. buf
        if response_length > 0 then
          if #(self.buffer) >= response_length then
            local response_buffer = string.sub(self.buffer, 1, response_length)
            self.buffer = string.sub(self.buffer, response_length + 1)
            response_length = 0
            self:_process_response(r._decode(response_buffer), token)
            if token == reqest_token then return end
          end
        else
          if #(self.buffer) >= 12 then
            token = bytes_to_int(self.buffer:sub(1, 8))
            response_length = bytes_to_int(self.buffer:sub(9, 12))
            self.buffer = self.buffer:sub(13)
          end
        end
      end
    end,
    _del_query = function(self, token)
      -- This query is done, delete this cursor
      if self.outstanding_callbacks[token].cursor then
        self.weight = self.weight - 1
      end
      self.outstanding_callbacks[token].cursor = nil
    end,
    _process_response = function(self, response, token)
      local cursor = self.outstanding_callbacks[token]
      if not cursor then
        -- Unexpected token
        return r._logger('Unexpected token ' .. token .. '.')
      end
      cursor = cursor.cursor
      if cursor then
        return cursor:_add_response(response)
      end
    end,
    close = function(self, opts_or_callback, callback)
      local opts = {}
      local cb
      if callback then
        if type(opts_or_callback) ~= 'table' then
          return r._logger('First argument to two-argument `close` must be a table.')
        end
        opts = opts_or_callback
        cb = callback
      elseif type(opts_or_callback) == 'table' then
        opts = opts_or_callback
      elseif type(opts_or_callback) == 'function' then
        cb = opts_or_callback
      end

      function wrapped_cb(err)
        self.open = false
        self.raw_socket:shutdown()
        self.raw_socket:close()
        if cb then
          return cb(err)
        end
        return nil, err
      end

      local noreply_wait = opts.noreply_wait and self.open

      if noreply_wait then
        return self:noreply_wait(wrapped_cb)
      end
      return wrapped_cb()
    end,
    noreply_wait = function(self, callback)
      local cb = function(err, cur)
        self.weight = 0
        if cur then
          return cur.next(function(err) return callback(err) end)
        end
        return callback(err)
      end
      if not self.open then
        return cb(errors.ReQLDriverError('Connection is closed.'))
      end

      -- Assign token
      local token = self.next_token
      self.next_token = self.next_token + 1

      -- Save cursor
      local cursor = Cursor(self, token, {})

      -- Save cursor
      self.outstanding_callbacks[token] = {cursor = cursor}

      -- Construct query
      self:_send_query(token, {--[[Query.NOREPLY_WAIT]]})

      return cb(nil, cursor)
    end,
    reconnect = function(self, opts_or_callback, callback)
      local opts = {}
      if callback or not type(opts_or_callback) == 'function' then
        opts = opts_or_callback
      else
        callback = opts_or_callback
      end
      return self:close(opts, function()
        return r.connect(self, callback)
      end)
    end,
    use = function(self, db)
      self.db = db
    end,
    _start = function(self, term, callback, opts)
      local cb = function(err, cur)
        local res
        if type(callback) == 'function' then
          res = callback(err, cur)
        else
          if err then
            return r._logger(err.message)
          end
        end
        cur:close()
        return res
      end
      if not self.open then
        cb(errors.ReQLDriverError('Connection is closed.'))
      end

      -- Assign token
      local token = self.next_token
      self.next_token = self.next_token + 1
      self.weight = self.weight + 1

      -- Set global options
      local global_opts = {}

      for k, v in pairs(opts) do
        global_opts[k] = r(v):build()
      end

      if opts.db then
        global_opts.db = r.db(opts.db):build()
      elseif self.db then
        global_opts.db = r.db(self.db):build()
      end

      if type(callback) ~= 'function' then
        global_opts.noreply = true
      end

      -- Construct query
      local query = {--[[Query.START]], term:build(), global_opts}

      local cursor = Cursor(self, token, opts, term)

      -- Save cursor
      self.outstanding_callbacks[token] = {cursor = cursor}
      self:_send_query(token, query)
      return cb(nil, cursor)
    end,
    _continue_query = function(self, token)
      self:_send_query(token, {--[[Query.CONTINUE]]})
    end,
    _end_query = function(self, token)
      self:_del_query(token)
      self:_send_query(token, {--[[Query.STOP]]})
    end,
    _send_query = function(self, token, query)
      local data = r._encode(query)
      self.raw_socket:send(
        int_to_bytes(token, 8) ..
        int_to_bytes(#data, 4) ..
        data
      )
    end
  }
)

r.pool = class(
  'Pool',
  {
    __init = function(self, host, callback)
      local cb = function(err, pool)
        if not r._pool then
          r._pool = pool
        end
        if callback then
          local res = callback(err, pool)
          pool:close({noreply_wait = false})
          return res
        end
        return pool, err
      end
      self.open = false
      conn, err = r.connect(host)
      if err then return cb(err) end
      self.open = true
      self.pool = {conn}
      self.size = host.size or 12
      self.host = host
      for i=2, self.size do
        table.insert(self.pool, (r.connect(host)))
      end
      return cb(nil, self)
    end,
    close = function(self, opts, callback)
      local err
      local cb = function(e)
        if e then
          err = e
        end
      end
      for _, conn in pairs(self.pool) do
        conn:close(opts, cb)
      end
      self.open = false
      if callback then return callback(err) end
    end,
    _start = function(self, term, callback, opts)
      local weight = math.huge
      local good_conn
      if opts.conn then
        weight = -1
        good_conn = self.pool[opts.conn]
      end
      for i=1, self.size do
        if not self.pool[i] then self.pool[i] = r.connect(self.host) end
        local conn = self.pool[i]
        if not conn.open then conn:reconnect() end
        if conn.weight < weight then
          good_conn = conn
          weight = conn.weight
        end
      end
      return good_conn:_start(term, callback, opts)
    end
  }
)

-- Export all names defined on r
return r
