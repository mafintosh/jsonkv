const nanoiterator = require('nanoiterator')
const raf = require('random-access-file')
const bulk = require('bulk-write-stream')

const BUCKET_SIZE = 65536
const READ_BUFFER = 64
const WRITE_BUFFER = 256

module.exports = createWriteStream

function createWriteStream (filename) {
  const tmp = raf(filename + '.tmp', {truncate: true})
  const buckets = []
  var next = createBucket()

  return bulk.obj({highWaterMark: BUCKET_SIZE}, batch, flush)

  function batch (data, cb) {
    for (var i = 0; i < data.length; i++) next.push(data[i])
    if (next.values.length < BUCKET_SIZE) return cb(null)

    next.flush(cb) // always nextticked
    next = createBucket()
  }

  function flush (cb) {
    next.flush(function (err) {
      if (err) return cb(err)
      mergeSort(cb)
    })
  }

  function mergeSort (cb) {
    const iterators = buckets.map(toIterator)
    var missing = iterators.length
    var error = null

    for (var i = 0; i < iterators.length; i++) iterators[i].next(ondone)

    function ondone (err) {
      if (err) error = err
      if (--missing) return
      if (error) return cb(error)

      const iterator = reduceIterators(iterators)
      const valueSize = buckets.map(a => a.valueSize).reduce((a, b) => Math.max(a, b))
      const storage = raf(filename, {truncate: true})
      const values = buckets.map(a => a.size).reduce((a, b) => a + b)

      var offset = 66 // after the header
      var block = allocSpaces(WRITE_BUFFER * (valueSize + 2))
      var ptr = 0

      const header = allocSpaces(66) // 64 (+EOL) byte header, fixed
      header.write(JSON.stringify({valueSize, values}), 0)
      header.write('\r\n', 64)
      storage.write(0, header, loop)

      function loop (err) {
        if (err) return flush(err)
        if (!iterator.value) return flush(null)

        block.write(JSON.stringify(iterator.value), ptr)
        ptr += valueSize
        block.write('\r\n', ptr)
        ptr += 2

        if (ptr === block.length) {
          ptr = 0
          storage.write(offset, block, nextAndLoop)
          offset += block.length
          return
        }

        iterator.next(loop)
      }

      function nextAndLoop (err) {
        if (err) return flush(err)
        iterator.next(loop)
      }

      function flush (err) {
        if (err || ptr === 0) return afterWrite(err)
        storage.write(offset, block.slice(0, ptr), afterWrite)
      }

      function afterWrite (err) {
        tmp.destroy(function () {
          if (err) return storage.destroy(_ => cb(err))
          storage.close(cb)
        })
      }
    }
  }

  function createBucket () {
    const start = buckets.length ? buckets[buckets.length - 1].end : 0
    const bucket = new Bucket(tmp, start)
    buckets.push(bucket)
    return bucket
  }
}

function emptyIterator () {
  const ite = nanoiterator({ next: cb => cb(null, null) })
  ite.value = null
  return ite
}

function reduceIterators (iterators) {
  while (iterators.length > 1) {
    const tmp = []

    for (var i = 0; i < iterators.length; i += 2) {
      const left = iterators[i]
      const right = i + 1 < iterators.length ? iterators[i + 1] : emptyIterator()
      const ite = nanoiterator({
        next (cb) {
          if (!ite.value) return cb(null, null)
          if (ite.value === left.value) return left.next(done)
          right.next(done)

          function done (err) {
            if (err) return cb(err)
            updateValue(ite, left, right)
            cb(null, ite.value)
          }
        }
      })

      updateValue(ite, left, right)
      tmp.push(ite)
    }

    iterators = tmp
  }

  return iterators[0]
}

function updateValue (ite, left, right) {
  if (!left.value && !right.value) ite.value = null
  else if (!left.value) ite.value = right.value
  else if (!right.value) ite.value = left.value
  else if (sort(left.value, right.value) < 0) ite.value = left.value
  else ite.value = right.value
}

function toIterator (bucket) {
  return bucket.iterate()
}

class Bucket {
  constructor (storage, start) {
    this.storage = storage
    this.values = []
    this.start = start
    this.end = start
    this.valueSize = 0
    this.size = 0
  }

  iterate () {
    if (this.values) throw new Error('Flush the bucket first')

    var offset = this.start
    var block = Buffer.alloc(0)
    var ptr = 0

    const end = this.end
    const storage = this.storage
    const valueSize = this.valueSize
    const blockSize = valueSize * READ_BUFFER

    const ite = nanoiterator({
      next (cb) {
        if (offset < end) {
          if (ptr < block.length) {
            push()
            cb(null, ite.value)
          } else {
            storage.read(offset, Math.min(blockSize, end - offset), onblock)
          }
        } else {
          ite.value = null
          cb(null, null)
        }

        function onblock (err, buf) {
          if (err) return cb(err)
          block = buf
          ptr = 0
          push()
          cb(null, ite.value)
        }
      }
    })

    ite.value = null
    return ite

    function push () {
      const str = block.toString('utf-8', ptr, ptr += valueSize)
      ite.value = JSON.parse(str)
      offset += valueSize
    }
  }

  push (val) {
    this.size++
    this.values.push(val)
  }

  flush (cb) {
    this.values.sort(sort)

    var i
    var maxSize = 0
    const encoded = []

    for (i = 0; i < this.size; i++) {
      const enc = JSON.stringify(this.values[i])
      const len = Buffer.byteLength(enc)

      if (len > maxSize) maxSize = len
      encoded.push(enc)
    }

    this.valueSize = maxSize
    this.values = null

    const buf = allocSpaces(maxSize * this.size)

    for (i = 0; i < this.size; i++) {
      buf.write(encoded[i], maxSize * i)
    }

    this.end += maxSize * this.size
    this.storage.write(this.start, buf, cb)
  }
}

function sort (a, b) {
  return a.key.localeCompare(b.key)
}

function allocSpaces (n) {
  const buf = Buffer.allocUnsafe(n)
  buf.fill(32) // spaces
  return buf
}
