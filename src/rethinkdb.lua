local class = require'rethinkdb.class'
local convert_pseudotype = require'rethinkdb.convert_pseudotype'
local Cursor = require'rethinkdb.cursor'

-- r is both the main export table for the module
-- and a function that wraps a native Lua value in a ReQL datum
local r = {
  is_instance = require'rethinkdb.is_instance'
}

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
  elseif not r._lib_mime then
    r._lib_mime = require('mime')
  end
  r.unb64 = r._lib_mime.unb64
  return r._lib_mime.unb64(data)
end

function r._b64(data)
  if r.b64 then
    return r.b64(data)
  elseif not r._lib_mime then
    r._lib_mime = require('mime')
  end
  r.b64 = r._lib_mime.b64
  return r._lib_mime.b64(data)
end

function r._encode(data)
  if r.encode then
    return r.encode(data)
  elseif r.json_parser then
    r.encode = r.json_parser.encode
    return r.json_parser.encode(data)
  elseif not r._lib_json then
    r._lib_json = require('json')
  end
  r.encode = r._lib_json.encode
  r.json_parser = r._lib_json
  return r._lib_json.encode(data)
end

function r._decode(buffer)
  if r.decode then
    return r.decode(buffer)
  elseif r.json_parser then
    r.decode = r.json_parser.decode
    return r.json_parser.decode(buffer)
  elseif not r._lib_json then
    r._lib_json = require('json')
  end
  r.decode = r._lib_json.decode
  r.json_parser = r._lib_json
  return r._lib_json.decode(buffer)
end

function r._socket()
  if r.socket then
    return r.socket()
  elseif not r._lib_socket then
    r._lib_socket = require('socket')
  end
  r.socket = r._lib_socket.tcp
  return r._lib_socket.tcp()
end

local DATUMTERM, ReQLOp
local AND, APPEND, ARGS, ASC, BETWEEN, BETWEEN_DEPRECATED, BINARY, BRACKET
local BRANCH, CHANGES, CHANGE_AT, COERCE_TO, CONCAT_MAP, CONFIG, CONTAINS
local COUNT, DB, DB_CREATE, DB_DROP, DB_LIST, DEFAULT, DELETE, DELETE_AT, DESC
local DIFFERENCE, DISTINCT, DOWNCASE, EQ, EQ_JOIN, ERROR, FILTER, FOR_EACH
local FUNC, FUNCALL, GET, GET_ALL, GET_FIELD, GROUP, HAS_FIELDS, HTTP
local INDEX_CREATE, INDEX_DROP, INDEX_LIST, INDEX_RENAME, INDEX_STATUS
local INDEX_WAIT, INFO, INNER_JOIN, INSERT, INSERT_AT, IS_EMPTY, JAVASCRIPT
local JSON, KEYS, LIMIT, LITERAL, MAKE_ARRAY, MAKE_OBJ, MAP, MATCH, MAXVAL
local MERGE, MINVAL, NE, NTH, OBJECT, OFFSETS_OF, OR, ORDER_BY, OUTER_JOIN
local PLUCK, PREPEND, RANDOM, RANGE, REBALANCE, RECONFIGURE, REDUCE, REPLACE
local SAMPLE, SET_DIFFERENCE, SET_INSERT, SET_INTERSECTION, SET_UNION, SKIP
local SLICE, SPLICE_AT, SPLIT, STATUS, SYNC, TABLE, TABLE_CREATE, TABLE_DROP
local TABLE_LIST, TO_JSON_STRING, TYPE_OF, UNGROUP, UNION, UPCASE, UPDATE
local UUID, VALUES, VAR, WAIT, WITHOUT, WITH_FIELDS, ZIP
local ReQLAuthError, ReQLAvailabilityError, ReQLClientError, ReQLCompileError
local ReQLDriverError, ReQLError, ReQLInternalError, ReQLNonExistenceError
local ReQLOpFailedError, ReQLOpIndeterminateError, ReQLQueryLogicError
local ReQLQueryPrinter, ReQLResourceLimitError, ReQLRuntimeError
local ReQLServerError, ReQLTimeoutError, ReQLUserError

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

ReQLError = class(
  'ReQLError',
  function(self, msg, term, frames)
    self.msg = msg
    self.message = function()
      if self._message then return self._message end
      self._message = self.__class.__name .. ' ' .. msg
      if term then
        self._message = self._message .. ' in:\n' .. ReQLQueryPrinter(term, frames):print_query()
      end
      return self._message
    end
  end
)

ReQLDriverError = class('ReQLDriverError', ReQLError, {})
ReQLServerError = class('ReQLServerError', ReQLError, {})

ReQLRuntimeError = class('ReQLRuntimeError', ReQLServerError, {})
ReQLCompileError = class('ReQLCompileError', ReQLServerError, {})

ReQLAuthError = class('ReQLDriverError', ReQLDriverError, {})

ReQLClientError = class('ReQLClientError', ReQLServerError, {})

ReQLAvailabilityError = class('ReQLRuntimeError', ReQLRuntimeError, {})
ReQLInternalError = class('ReQLRuntimeError', ReQLRuntimeError, {})
ReQLQueryLogicError = class('ReQLRuntimeError', ReQLRuntimeError, {})
ReQLResourceLimitError = class('ReQLRuntimeError', ReQLRuntimeError, {})
ReQLTimeoutError = class('ReQLRuntimeError', ReQLRuntimeError, {})
ReQLUserError = class('ReQLRuntimeError', ReQLRuntimeError, {})

ReQLOpFailedError = class('ReQLRuntimeError', ReQLAvailabilityError, {})
ReQLOpIndeterminateError = class('ReQLRuntimeError', ReQLAvailabilityError, {})

ReQLNonExistenceError = class('ReQLRuntimeError', ReQLQueryLogicError, {})

ReQLQueryPrinter = class(
  'ReQLQueryPrinter',
  {
    __init = function(self, term, frames)
      self.term = term
      self.frames = frames
    end,
    print_query = function(self)
      local carrots
      if next(self.frames) then
        carrots = self:compose_carrots(self.term, self.frames)
      else
        carrots = {self:carrotify(self:compose_term(self.term))}
      end
      carrots = self:join_tree(carrots):gsub('[^%^]', '')
      return self:join_tree(self:compose_term(self.term)) .. '\n' .. carrots
    end,
    compose_term = function(self, term)
      if type(term) ~= 'table' then return '' .. term end
      local args = {}
      for i, arg in ipairs(term.args) do
        args[i] = self:compose_term(arg)
      end
      local optargs = {}
      for key, arg in pairs(term.optargs) do
        optargs[key] = self:compose_term(arg)
      end
      return term:compose(args, optargs)
    end,
    compose_carrots = function(self, term, frames)
      local frame = table.remove(frames, 1)
      local args = {}
      for i, arg in ipairs(term.args) do
        if frame == (i - 1) then
          args[i] = self:compose_carrots(arg, frames)
        else
          args[i] = self:compose_term(arg)
        end
      end
      local optargs = {}
      for key, arg in pairs(term.optargs) do
        if frame == key then
          optargs[key] = self:compose_carrots(arg, frames)
        else
          optargs[key] = self:compose_term(arg)
        end
      end
      if frame then
        return term:compose(args, optargs)
      end
      return self:carrotify(term:compose(args, optargs))
    end,
    carrot_marker = {},
    carrotify = function(self, tree)
      return {carrot_marker, tree}
    end,
    join_tree = function(self, tree)
      local str = ''
      for _, term in ipairs(tree) do
        if type(term) == 'table' then
          if #term == 2 and term[1] == self.carrot_marker then
            str = str .. self:join_tree(term[2]):gsub('.', '^')
          else
            str = str .. self:join_tree(term)
          end
        else
          str = str .. term
        end
      end
      return str
    end
  }
)

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
        return error('Second argument to `run` cannot be a function if a third argument is provided.')
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
          return callback(ReQLDriverError('First argument to `run` must be a connection.'))
        end
        return error('First argument to `run` must be a connection.')
      end
    end

    return connection:_start(self, callback, options or {})
  end,
  and_ = function(...) return AND({}, ...) end,
  append = function(...) return APPEND({}, ...) end,
  args = function(...) return ARGS({}, ...) end,
  asc = function(...) return ASC({}, ...) end,
  between = function(arg0, arg1, arg2, opts) return BETWEEN(opts, arg0, arg1, arg2) end,
  between_deprecated = function(arg0, arg1, arg2, opts) return BETWEEN_DEPRECATED(opts, arg0, arg1, arg2) end,
  binary = function(...) return BINARY({}, ...) end,
  index = function(...) return BRACKET({}, ...) end,
  branch = function(...) return BRANCH({}, ...) end,
  changes = function(...) return CHANGES({}, ...) end,
  change_at = function(...) return CHANGE_AT({}, ...) end,
  coerce_to = function(...) return COERCE_TO({}, ...) end,
  concat_map = function(...) return CONCAT_MAP({}, ...) end,
  config = function(...) return CONFIG({}, ...) end,
  contains = function(...) return CONTAINS({}, ...) end,
  count = function(...) return COUNT({}, ...) end,
  db = function(...) return DB({}, ...) end,
  db_create = function(...) return DB_CREATE({}, ...) end,
  db_drop = function(...) return DB_DROP({}, ...) end,
  db_list = function(...) return DB_LIST({}, ...) end,
  default = function(...) return DEFAULT({}, ...) end,
  delete = function(...) return DELETE(get_opts(...)) end,
  delete_at = function(...) return DELETE_AT({}, ...) end,
  desc = function(...) return DESC({}, ...) end,
  difference = function(...) return DIFFERENCE({}, ...) end,
  distinct = function(...) return DISTINCT(get_opts(...)) end,
  downcase = function(...) return DOWNCASE({}, ...) end,
  eq = function(...) return EQ({}, ...) end,
  eq_join = function(...) return EQ_JOIN(get_opts(...)) end,
  error_ = function(...) return ERROR({}, ...) end,
  filter = function(arg0, arg1, opts) return FILTER(opts, arg0, arg1) end,
  for_each = function(...) return FOR_EACH({}, ...) end,
  func = function(...) return FUNC({}, ...) end,
  do_ = function(...) return FUNCALL({}, ...) end,
  get = function(...) return GET({}, ...) end,
  get_all = function(...) return GET_ALL(get_opts(...)) end,
  get_field = function(...) return GET_FIELD({}, ...) end,
  group = function(...) return GROUP(get_opts(...)) end,
  has_fields = function(...) return HAS_FIELDS({}, ...) end,
  http = function(...) return HTTP(get_opts(...)) end,
  index_create = function(...) return INDEX_CREATE(get_opts(...)) end,
  index_drop = function(...) return INDEX_DROP({}, ...) end,
  index_list = function(...) return INDEX_LIST({}, ...) end,
  index_rename = function(...) return INDEX_RENAME(get_opts(...)) end,
  index_status = function(...) return INDEX_STATUS({}, ...) end,
  index_wait = function(...) return INDEX_WAIT({}, ...) end,
  info = function(...) return INFO({}, ...) end,
  inner_join = function(...) return INNER_JOIN({}, ...) end,
  insert = function(arg0, arg1, opts) return INSERT(opts, arg0, arg1) end,
  insert_at = function(...) return INSERT_AT({}, ...) end,
  is_empty = function(...) return IS_EMPTY({}, ...) end,
  js = function(...) return JAVASCRIPT(get_opts(...)) end,
  json = function(...) return JSON({}, ...) end,
  keys = function(...) return KEYS({}, ...) end,
  limit = function(...) return LIMIT({}, ...) end,
  literal = function(...) return LITERAL({}, ...) end,
  make_array = function(...) return MAKE_ARRAY({}, ...) end,
  make_obj = function(...) return MAKE_OBJ({}, ...) end,
  map = function(...) return MAP({}, ...) end,
  match = function(...) return MATCH({}, ...) end,
  maxval = function(...) return MAXVAL({}, ...) end,
  merge = function(...) return MERGE({}, ...) end,
  minval = function(...) return MINVAL({}, ...) end,
  ne = function(...) return NE({}, ...) end,
  nth = function(...) return NTH({}, ...) end,
  object = function(...) return OBJECT({}, ...) end,
  offsets_of = function(...) return OFFSETS_OF({}, ...) end,
  or_ = function(...) return OR({}, ...) end,
  order_by = function(...) return ORDER_BY(get_opts(...)) end,
  outer_join = function(...) return OUTER_JOIN({}, ...) end,
  pluck = function(...) return PLUCK({}, ...) end,
  prepend = function(...) return PREPEND({}, ...) end,
  random = function(...) return RANDOM(get_opts(...)) end,
  range = function(...) return RANGE({}, ...) end,
  rebalance = function(...) return REBALANCE({}, ...) end,
  reconfigure = function(...) return RECONFIGURE({}, ...) end,
  reduce = function(...) return REDUCE({}, ...) end,
  replace = function(...) return REPLACE(get_opts(...)) end,
  sample = function(...) return SAMPLE({}, ...) end,
  set_difference = function(...) return SET_DIFFERENCE({}, ...) end,
  set_insert = function(...) return SET_INSERT({}, ...) end,
  set_intersection = function(...) return SET_INTERSECTION({}, ...) end,
  set_union = function(...) return SET_UNION({}, ...) end,
  skip = function(...) return SKIP({}, ...) end,
  slice = function(...) return SLICE(get_opts(...)) end,
  splice_at = function(...) return SPLICE_AT({}, ...) end,
  split = function(...) return SPLIT({}, ...) end,
  status = function(...) return STATUS({}, ...) end,
  sync = function(...) return SYNC({}, ...) end,
  table = function(...) return TABLE(get_opts(...)) end,
  table_create = function(...) return TABLE_CREATE(get_opts(...)) end,
  table_drop = function(...) return TABLE_DROP({}, ...) end,
  table_list = function(...) return TABLE_LIST({}, ...) end,
  to_json_string = function(...) return TO_JSON_STRING({}, ...) end,
  type_of = function(...) return TYPE_OF({}, ...) end,
  ungroup = function(...) return UNGROUP({}, ...) end,
  union = function(...) return UNION({}, ...) end,
  upcase = function(...) return UPCASE({}, ...) end,
  update = function(arg0, arg1, opts) return UPDATE(opts, arg0, arg1) end,
  uuid = function(...) return UUID({}, ...) end,
  values = function(...) return VALUES({}, ...) end,
  var = function(...) return VAR({}, ...) end,
  wait = function(...) return WAIT({}, ...) end,
  without = function(...) return WITHOUT({}, ...) end,
  with_fields = function(...) return WITH_FIELDS({}, ...) end,
  zip = function(...) return ZIP({}, ...) end
}

class_methods = {
  __init = function(self, optargs, ...)
    local args = {...}
    optargs = optargs or {}
    if self.tt == 69 then
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
        return error('Anonymous function returned `nil`. Did you forget a `return`?')
      end
      optargs.arity = nil
      args = {arg_nums, func}
    elseif self.tt == 155 then
      local data = args[1]
      if r.is_instance(data, 'ReQLOp') then
      elseif type(data) == 'string' then
        self.base64_data = r._b64(table.remove(args, 1))
      else
        return error('Parameter to `r.binary` must be a string or ReQL query.')
      end
    elseif self.tt == 64 then
      local func = table.remove(args)
      if type(func) == 'function' then
        func = FUNC({arity = #args}, func)
      end
      table.insert(args, 1, func)
    elseif self.tt == 37 then
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
    if self.tt == 155 and (not self.args[1]) then
      return {
        ['$reql_type$'] = 'BINARY',
        data = self.base64_data
      }
    end
    if self.tt == 3 then
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
  compose = function(self, args, optargs)
    intsp = function(seq)
      local res = {}
      local sep = ''
      for _, v in ipairs(seq) do
        table.insert(res, {sep, v})
        sep = ', '
      end
      return res
    end
    if self.tt == 2 then
      return {
        '{',
        intsp(args),
        '}'
      }
    end
    kved = function(optargs)
      local res = {'{'}
      local sep = ''
      for k, v in pairs(optargs) do
        table.insert(res, {sep, k, ': ', v})
        sep = ', '
      end
      table.insert(res, '}')
      return res
    end
    if self.tt == 3 then
      return kved(optargs)
    end
    if self.tt == 10 then
      return {'var_' .. args[1]}
    end
    if self.tt == 155 and not self.args[1] then
      return 'r.binary(<data>)'
    end
    if self.tt == 170 then
      return {
        args[1],
        '(',
        args[2],
        ')'
      }
    end
    if self.tt == 69 then
      return {
        'function(',
        intsp((function()
          local _accum_0 = {}
          for i, v in ipairs(self.args[1]) do
            _accum_0[i] = 'var_' .. v
          end
          return _accum_0
        end)()),
        ') return ',
        args[2],
        ' end'
      }
    end
    if self.tt == 64 then
      local func = table.remove(args, 1)
      if func then
        table.insert(args, func)
      end
    end
    if not self.args then
      return {
        type(self)
      }
    end
    intspallargs = function(args, optargs)
      local argrepr = {}
      if args and next(args) then
        table.insert(argrepr, intsp(args))
      end
      if optargs and next(optargs) then
        if next(argrepr) then
          table.insert(argrepr, ', ')
        end
        table.insert(argrepr, kved(optargs))
      end
      return argrepr
    end
    return {
      'r.' .. self.st .. '(',
      intspallargs(args, optargs),
      ')'
    }
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
          return error('Illegal non-finite number `' .. val .. '`.')
        end
      end
      self.data = val
    end,
    args = {},
    optargs = {},
    compose = function(self)
      if self.data == nil then
        return 'nil'
      end
      return r._encode(self.data)
    end,
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

AND = ast('AND', {tt = 67, st = 'and_'})
APPEND = ast('APPEND', {tt = 29, st = 'append'})
ARGS = ast('ARGS', {tt = 154, st = 'args'})
ASC = ast('ASC', {tt = 73, st = 'asc'})
BETWEEN = ast('BETWEEN', {tt = 182, st = 'between'})
BETWEEN_DEPRECATED = ast('BETWEEN_DEPRECATED', {tt = 36, st = 'between_deprecated'})
BINARY = ast('BINARY', {tt = 155, st = 'binary'})
BRACKET = ast('BRACKET', {tt = 170, st = 'index'})
BRANCH = ast('BRANCH', {tt = 65, st = 'branch'})
CHANGES = ast('CHANGES', {tt = 152, st = 'changes'})
CHANGE_AT = ast('CHANGE_AT', {tt = 84, st = 'change_at'})
COERCE_TO = ast('COERCE_TO', {tt = 51, st = 'coerce_to'})
CONCAT_MAP = ast('CONCAT_MAP', {tt = 40, st = 'concat_map'})
CONFIG = ast('CONFIG', {tt = 174, st = 'config'})
CONTAINS = ast('CONTAINS', {tt = 93, st = 'contains'})
COUNT = ast('COUNT', {tt = 43, st = 'count'})
DB = ast('DB', {tt = 14, st = 'db'})
DB_CREATE = ast('DB_CREATE', {tt = 57, st = 'db_create'})
DB_DROP = ast('DB_DROP', {tt = 58, st = 'db_drop'})
DB_LIST = ast('DB_LIST', {tt = 59, st = 'db_list'})
DEFAULT = ast('DEFAULT', {tt = 92, st = 'default'})
DELETE = ast('DELETE', {tt = 54, st = 'delete'})
DELETE_AT = ast('DELETE_AT', {tt = 83, st = 'delete_at'})
DESC = ast('DESC', {tt = 74, st = 'desc'})
DIFFERENCE = ast('DIFFERENCE', {tt = 95, st = 'difference'})
DISTINCT = ast('DISTINCT', {tt = 42, st = 'distinct'})
DOWNCASE = ast('DOWNCASE', {tt = 142, st = 'downcase'})
EQ = ast('EQ', {tt = 17, st = 'eq'})
EQ_JOIN = ast('EQ_JOIN', {tt = 50, st = 'eq_join'})
ERROR = ast('ERROR', {tt = 12, st = 'error_'})
FILTER = ast('FILTER', {tt = 39, st = 'filter'})
FOR_EACH = ast('FOR_EACH', {tt = 68, st = 'for_each'})
FUNC = ast('FUNC', {tt = 69, st = 'func'})
FUNCALL = ast('FUNCALL', {tt = 64, st = 'do_'})
GET = ast('GET', {tt = 16, st = 'get'})
GET_ALL = ast('GET_ALL', {tt = 78, st = 'get_all'})
GET_FIELD = ast('GET_FIELD', {tt = 31, st = 'get_field'})
GROUP = ast('GROUP', {tt = 144, st = 'group'})
HAS_FIELDS = ast('HAS_FIELDS', {tt = 32, st = 'has_fields'})
HTTP = ast('HTTP', {tt = 153, st = 'http'})
INDEX_CREATE = ast('INDEX_CREATE', {tt = 75, st = 'index_create'})
INDEX_DROP = ast('INDEX_DROP', {tt = 76, st = 'index_drop'})
INDEX_LIST = ast('INDEX_LIST', {tt = 77, st = 'index_list'})
INDEX_RENAME = ast('INDEX_RENAME', {tt = 156, st = 'index_rename'})
INDEX_STATUS = ast('INDEX_STATUS', {tt = 139, st = 'index_status'})
INDEX_WAIT = ast('INDEX_WAIT', {tt = 140, st = 'index_wait'})
INFO = ast('INFO', {tt = 79, st = 'info'})
INNER_JOIN = ast('INNER_JOIN', {tt = 48, st = 'inner_join'})
INSERT = ast('INSERT', {tt = 56, st = 'insert'})
INSERT_AT = ast('INSERT_AT', {tt = 82, st = 'insert_at'})
IS_EMPTY = ast('IS_EMPTY', {tt = 86, st = 'is_empty'})
JAVASCRIPT = ast('JAVASCRIPT', {tt = 11, st = 'js'})
JSON = ast('JSON', {tt = 98, st = 'json'})
KEYS = ast('KEYS', {tt = 94, st = 'keys'})
LIMIT = ast('LIMIT', {tt = 71, st = 'limit'})
LITERAL = ast('LITERAL', {tt = 137, st = 'literal'})
MAKE_ARRAY = ast('MAKE_ARRAY', {tt = 2, st = 'make_array'})
MAKE_OBJ = ast('MAKE_OBJ', {tt = 3, st = 'make_obj'})
MAP = ast('MAP', {tt = 38, st = 'map'})
MATCH = ast('MATCH', {tt = 97, st = 'match'})
MAXVAL = ast('MAXVAL', {tt = 181, st = 'maxval'})
MERGE = ast('MERGE', {tt = 35, st = 'merge'})
MINVAL = ast('MINVAL', {tt = 180, st = 'minval'})
NE = ast('NE', {tt = 18, st = 'ne'})
NTH = ast('NTH', {tt = 45, st = 'nth'})
OBJECT = ast('OBJECT', {tt = 143, st = 'object'})
OFFSETS_OF = ast('OFFSETS_OF', {tt = 87, st = 'offsets_of'})
OR = ast('OR', {tt = 66, st = 'or_'})
ORDER_BY = ast('ORDER_BY', {tt = 41, st = 'order_by'})
OUTER_JOIN = ast('OUTER_JOIN', {tt = 49, st = 'outer_join'})
PLUCK = ast('PLUCK', {tt = 33, st = 'pluck'})
PREPEND = ast('PREPEND', {tt = 80, st = 'prepend'})
RANDOM = ast('RANDOM', {tt = 151, st = 'random'})
RANGE = ast('RANGE', {tt = 173, st = 'range'})
REBALANCE = ast('REBALANCE', {tt = 179, st = 'rebalance'})
RECONFIGURE = ast('RECONFIGURE', {tt = 176, st = 'reconfigure'})
REDUCE = ast('REDUCE', {tt = 37, st = 'reduce'})
REPLACE = ast('REPLACE', {tt = 55, st = 'replace'})
SAMPLE = ast('SAMPLE', {tt = 81, st = 'sample'})
SET_DIFFERENCE = ast('SET_DIFFERENCE', {tt = 91, st = 'set_difference'})
SET_INSERT = ast('SET_INSERT', {tt = 88, st = 'set_insert'})
SET_INTERSECTION = ast('SET_INTERSECTION', {tt = 89, st = 'set_intersection'})
SET_UNION = ast('SET_UNION', {tt = 90, st = 'set_union'})
SKIP = ast('SKIP', {tt = 70, st = 'skip'})
SLICE = ast('SLICE', {tt = 30, st = 'slice'})
SPLICE_AT = ast('SPLICE_AT', {tt = 85, st = 'splice_at'})
SPLIT = ast('SPLIT', {tt = 149, st = 'split'})
STATUS = ast('STATUS', {tt = 175, st = 'status'})
SYNC = ast('SYNC', {tt = 138, st = 'sync'})
TABLE = ast('TABLE', {tt = 15, st = 'table'})
TABLE_CREATE = ast('TABLE_CREATE', {tt = 60, st = 'table_create'})
TABLE_DROP = ast('TABLE_DROP', {tt = 61, st = 'table_drop'})
TABLE_LIST = ast('TABLE_LIST', {tt = 62, st = 'table_list'})
TO_JSON_STRING = ast('TO_JSON_STRING', {tt = 172, st = 'to_json_string'})
TYPE_OF = ast('TYPE_OF', {tt = 52, st = 'type_of'})
UNGROUP = ast('UNGROUP', {tt = 150, st = 'ungroup'})
UNION = ast('UNION', {tt = 44, st = 'union'})
UPCASE = ast('UPCASE', {tt = 141, st = 'upcase'})
UPDATE = ast('UPDATE', {tt = 53, st = 'update'})
UUID = ast('UUID', {tt = 169, st = 'uuid'})
VALUES = ast('VALUES', {tt = 186, st = 'values'})
VAR = ast('VAR', {tt = 10, st = 'var'})
WAIT = ast('WAIT', {tt = 177, st = 'wait'})
WITHOUT = ast('WITHOUT', {tt = 34, st = 'without'})
WITH_FIELDS = ast('WITH_FIELDS', {tt = 96, st = 'with_fields'})
ZIP = ast('ZIP', {tt = 72, st = 'zip'})

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
      self.weight = 0
      self.host = host.host or self.DEFAULT_HOST
      self.port = host.port or self.DEFAULT_PORT
      self.db = host.db -- left nil if this is not set
      self.auth_key = host.auth_key or self.DEFAULT_AUTH_KEY
      self.timeout = host.timeout or self.DEFAULT_TIMEOUT
      self.outstanding_callbacks = {}
      self.next_token = 1
      self.buffer = ''
      if self.raw_socket then
        self:close({noreply_wait = false})
      end
      return self:_connect(callback)
    end,
    _connect = function(self, callback)
      local cb = function(err, conn)
        if callback then
          local res = callback(err, conn)
          conn:close({noreply_wait = false})
          return res
        end
        return conn, err
      end
      self.raw_socket = r._socket()
      self.raw_socket:settimeout(self.timeout)
      local status, err = self.raw_socket:connect(self.host, self.port)
      if status then
        local buf, err, partial
        -- Initialize connection with magic number to validate version
        self.raw_socket:send(
          '\32\45\12\64' ..
          self.int_to_bytes(#(self.auth_key), 4) ..
          self.auth_key ..
          '\199\112\105\126'
        )

        -- Now we have to wait for a response from the server
        -- acknowledging the connection
        while 1 do
          buf, err, partial = self.raw_socket:receive(8)
          buf = buf or partial
          if not buf then
            return cb(ReQLDriverError('Server dropped connection with message:  \'' .. status_str .. '\'\n' .. err))
          end
          self.buffer = self.buffer .. buf
          i, j = buf:find('\0')
          if i then
            local status_str = self.buffer:sub(1, i - 1)
            self.buffer = self.buffer:sub(i + 1)
            if status_str == 'SUCCESS' then
              -- We're good, finish setting up the connection
              return cb(nil, self)
            end
            return cb(ReQLDriverError('Server dropped connection with message: \'' .. status_str .. '\''))
          end
        end
      end
      return cb(ReQLDriverError('Could not connect to ' .. self.host .. ':' .. self.port .. '.\n' .. err))
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
          self:close({noreply_wait = false})
          return self:_process_response(
            {
              t = 16,
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
            self:_continue_query(token)
            self:_process_response(r._decode(response_buffer), token)
            if token == reqest_token then return end
          end
        else
          if #(self.buffer) >= 12 then
            token = self.bytes_to_int(self.buffer:sub(1, 8))
            response_length = self.bytes_to_int(self.buffer:sub(9, 12))
            self.buffer = self.buffer:sub(13)
          end
        end
      end
    end,
    _del_query = function(self, token)
      -- This query is done, delete this cursor
      if not self.outstanding_callbacks[token] then return end
      if self.outstanding_callbacks[token].cursor then
        if self.outstanding_callbacks[token].cursor._type ~= 'finite' then
          self.weight = self.weight - 2
        end
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
          return error('First argument to two-argument `close` must be a table.')
        end
        opts = opts_or_callback
        cb = callback
      elseif type(opts_or_callback) == 'table' then
        opts = opts_or_callback
      elseif type(opts_or_callback) == 'function' then
        cb = opts_or_callback
      end

      function wrapped_cb(err)
        if self.raw_socket then
          self.raw_socket:shutdown()
          self.raw_socket:close()
          self.raw_socket = nil
        end
        if cb then
          return cb(err)
        end
        return nil, err
      end

      local noreply_wait = (opts.noreply_wait ~= false) and self:open()

      if noreply_wait then
        return self:noreply_wait(wrapped_cb)
      end
      return wrapped_cb()
    end,
    open = function(self)
      if self.raw_socket then
        return true
      end
      return false
    end,
    noreply_wait = function(self, callback)
      local cb = function(err, cur)
        if cur then
          return cur:next(function(err)
            self.weight = 0
            for token, cur in pairs(self.outstanding_callbacks) do
              if cur.cursor then
                self.weight = self.weight + 3
              else
                self.outstanding_callbacks[token] = nil
              end
            end
            return callback(err)
          end)
        end
        return callback(err)
      end
      if not self:open() then
        return cb(ReQLDriverError('Connection is closed.'))
      end

      -- Assign token
      local token = self.next_token
      self.next_token = self.next_token + 1

      -- Save cursor
      local cursor = Cursor(self, token, {})

      -- Save cursor
      self.outstanding_callbacks[token] = {cursor = cursor}

      -- Construct query
      self:_write_socket(token, {4})

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
        return self:_connect(callback)
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
      if not self:open() then
        return cb(ReQLDriverError('Connection is closed.'))
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
      local query = {1, term:build(), global_opts}

      local idx, err = self:_write_socket(token, query)
      if err then
        self:close({noreply_wait = false}, function(err)
          if err then return cb(err) end
          return cb(ReQLDriverError('Connection is closed.'))
        end)
      end
      local cursor = Cursor(self, token, opts, term)
      -- Save cursor
      self.outstanding_callbacks[token] = {cursor = cursor}
      return cb(nil, cursor)
    end,
    _continue_query = function(self, token)
      self:_write_socket(token, {2})
    end,
    _end_query = function(self, token)
      self:_del_query(token)
      self:_write_socket(token, {3})
    end,
    _write_socket = function(self, token, query)
      if not self.raw_socket then return nil, 'closed' end
      local data = r._encode(query)
      return self.raw_socket:send(
        self.int_to_bytes(token, 8) ..
        self.int_to_bytes(#data, 4) ..
        data
      )
    end,
    bytes_to_int = function(str)
      local t = {str:byte(1,-1)}
      local n = 0
      for k=1,#t do
        n = n + t[k] * 2 ^ ((k - 1) * 8)
      end
      return n
    end,
    int_to_bytes = function(num, bytes)
      local res = {}
      local mul = 0
      for k = bytes, 1, -1 do
        local den = 2 ^ (8 * (k - 1))
        res[k] = math.floor(num / den)
        num = math.fmod(num, den)
      end
      return string.char(unpack(res))
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
      self._open = false
      return r.connect(host, function(err, conn)
        if err then return cb(err) end
        self._open = true
        self.pool = {conn}
        self.size = host.size or 12
        self.host = host
        for i=2, self.size do
          table.insert(self.pool, (r.connect(host)))
        end
        return cb(nil, self)
      end)
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
      self._open = false
      if callback then return callback(err) end
    end,
    open = function(self)
      if not self._open then return false end
      for _, conn in ipairs(self.pool) do
        if conn:open() then return true end
      end
      self._open = false
      return false
    end,
    _start = function(self, term, callback, opts)
      local weight = math.huge
      if opts.conn then
        local good_conn = self.pool[opts.conn]
        if good_conn then
          return good_conn:_start(term, callback, opts)
        end
      end
      local good_conn
      for i=1, self.size do
        if not self.pool[i] then self.pool[i] = r.connect(self.host) end
        local conn = self.pool[i]
        if not conn:open() then
          conn:reconnect()
          self.pool[i] = conn
        end
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
