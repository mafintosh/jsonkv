const raf = require('random-access-file')
const nanoiterator = require('nanoiterator')
const toStream = require('nanoiterator/to-stream')

jsonkv.createWriteStream = require('./write-stream')
module.exports = jsonkv

function jsonkv (filename, opts) {
  return new DB(filename, opts)
}

class DB {
  constructor (filename, opts) {
    if (typeof opts === 'function') opts = {sort: opts}
    if (!opts) opts = {}
    this.sort = opts.sort || sortByKey
    this.storage = raf(filename)
    this.valueSize = 0
    this.length = 0
    this.offset = 0
    this.opened = false
  }

  open (cb) {
    if (!cb) cb = noop
    if (this.opened) return cb(null)

    const self = this

    this.storage.stat(function (err, st) {
      if (err) return cb(err)

      const headerSize = Math.min(st.size, 128)
      if (!headerSize) return cb(new Error('Database file should not empty'))

      self.storage.read(0, headerSize, function (err, buf) {
        if (err) return cb(err)

        // 91 is [
        const idx = buf.lastIndexOf
          ? buf.lastIndexOf(91)
          : buf.indexOf(91)

        const header = decode(buf.toString('utf-8', 0, idx + 1) + ']}')
        if (!header) return cb(new Error('Database has an invalid header'))

        self.valueSize = header.valueSize
        self.length = header.length
        self.offset = idx + 2 + 1
        self.opened = true

        cb(null)
      })
    })
  }

  createReadStream (opts) {
    return toStream(this.iterator(opts))
  }

  iterate (opts) {
    if (!opts) opts = {}

    const self = this
    const g = opts.gte || opts.gt
    const l = opts.lte || opts.lt
    const start = g && typeof g === 'object' ? g : {key: g}
    const end = l && typeof l === 'object' ? l : {key: l}

    var offset = 0
    var limit = 0

    return nanoiterator({
      open,
      next
    })

    function open (cb) {
      if (!start) return self.open(cb)
      self.get(start, {closest: true}, onstart)

      function onstart (err, val, seq) {
        if (err) return cb(err)

        offset = self.offset + seq * (4 + self.valueSize + 1 + 2)
        limit = val ? self.length - seq : 0
        if (limit <= 0) return cb(null)

        const cmp = self.sort(val, start)
        if (opts.gte && cmp >= 0) return cb(null)
        if (opts.gt && cmp > 0) return cb(null)

        offset += (4 + self.valueSize + 1 + 2)
        next((err, val) => onstart(err, val, seq + 1))
      }
    }

    function next (cb) {
      if (!offset) {
        offset = self.offset
        limit = self.length
      }

      if (!limit) return cb(null, null)

      self.storage.read(offset, 4 + self.valueSize, function (err, buf) {
        if (err) return cb(err)

        offset += 4 + self.valueSize + 1 + 2
        limit--

        const val = decodeValue(buf)
        if (!val) return cb(new Error('Invalid database entry'))

        if (end) {
          const cmp = self.sort(val, end)
          if (opts.lte && cmp > 0) return cb(null, null)
          if (opts.lt && cmp >= 0) return cb(null, null)
        }

        cb(null, val)
      })
    }
  }

  get (key, opts, cb) {
    if (typeof opts === 'function') return this.get(key, null, opts)
    if (!this.opened) return openAndGet(this, key, opts, cb)

    const self = this
    const target = typeof key === 'object' ? key : {key}
    const closest = !!(opts && opts.closest)

    var top = this.length
    var btm = 0
    var mid = Math.floor((top + btm) / 2)

    getValue(this, mid, function loop (err, val) {
      if (err) return cb(err)

      const cmp = self.sort(target, val)

      if (!cmp) return cb(null, val, mid)
      if (top - btm <= 1) return cb(null, closest ? val : null, mid)

      if (cmp < 0) top = mid
      else btm = mid

      mid = Math.floor((top + btm) / 2)
      getValue(self, mid, loop)
    })
  }
}

function getValue (db, idx, cb) {
  const size = 4 + db.valueSize + 1 + 2
  const offset = db.offset + idx * size

  db.storage.read(offset, size, function (err, buf) {
    if (err) return cb(err)
    const val = decodeValue(buf)
    if (!val) return cb(new Error('Invalid database entry'))
    cb(null, val)
  })
}

function decodeValue (buf) {
  const val = decode('[' + buf + 'null]')
  if (val) return val[0]
  return decode(buf.toString())
}

function openAndGet (db, key, opts, cb) {
  db.open(function (err) {
    if (err) return cb(err)
    db.get(key, opts, cb)
  })
}

function decode (str) {
  try {
    return JSON.parse(str)
  } catch (err) {
    return null
  }
}

function noop () {}

function sortByKey (a, b) {
  if (a.key === b.key) return 0
  return a.key < b.key ? -1 : 1
}
