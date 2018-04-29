const raf = require('random-access-file')

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

  get (key, cb) {
    if (!this.opened) return openAndGet(this, key, cb)

    const self = this
    const target = typeof key === 'object' ? key : {key}

    var top = this.length - 1
    var btm = 0
    var mid = Math.floor((top + btm) / 2)

    getValue(this, mid, function loop (err, val) {
      if (err) return cb(err)

      const cmp = self.sort(target, val)
      if (!cmp) return cb(null, val)
      if (top <= btm) return cb(null, null)

      if (cmp < 0) top = mid
      else btm = mid

      const next = Math.floor((top + btm) / 2)

      if (next === mid) {
        mid = next + 1
        top = btm
      } else {
        mid = next
      }

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
  const val = decode('[' + buf + '{}]')
  if (val) return val[0]
  return decode(buf.toString())
}

function openAndGet (db, key, cb) {
  db.open(function (err) {
    if (err) return cb(err)
    db.get(key, cb)
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
