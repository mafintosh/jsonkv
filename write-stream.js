const nanoiterator = require('nanoiterator')
const raf = require('random-access-file')
const bulk = require('bulk-write-stream')

const BUCKET_SIZE = 65536
const READ_BUFFER = 64
const WRITE_BUFFER = 256

module.exports = createWriteStream

function createWriteStream (filename, sort) {
  if (!sort) sort = sortByKey

  const tmp = raf(filename + '.tmp', {truncate: true})
  const buckets = []
  var next = createBucket()

  return bulk.obj({highWaterMark: BUCKET_SIZE}, batch, flush)

  function batch (data, cb) {
    for (var i = 0; i < data.length; i++) next.push(data[i])
    if (next.size < BUCKET_SIZE) return cb(null)
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

      const iterator = reduceIterators(iterators, sort)
      const valueSize = buckets.map(a => a.valueSize).reduce((a, b) => Math.max(a, b))
      const storage = raf(filename, {truncate: true})

      var length = buckets.map(a => a.size).reduce((a, b) => a + b)
      var offset = 0 // after the header
      var block = Buffer.allocUnsafe(WRITE_BUFFER * (4 + valueSize + 1 + 2))
      var ptr = 0

      const header = Buffer.from(
        `{\r\n  "valueSize": ${valueSize},\r\n  "length": ${length},\r\n  "values": [\r\n`
      )

      offset = header.length
      storage.write(0, header, loop)

      function loop (err) {
        if (err) return flush(err)
        if (!iterator.value) return flush(null)

        length--
        const sep = length ? ',' : ' '
        const wrote = block.write('    ' + JSON.stringify(iterator.value) + sep, ptr)
        const end = ptr + 4 + valueSize + 1

        block.fill(32, ptr + wrote, end)
        block.write('\r\n', end)
        ptr = end + 2

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
        if (err) return afterWrite(err)
        const end = Buffer.from('  ]\r\n}\r\n')
        const buf = Buffer.concat([block.slice(0, ptr), end])
        storage.write(offset, buf, afterWrite)
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
    const bucket = new Bucket(tmp, start, sort)
    buckets.push(bucket)
    return bucket
  }
}

function emptyIterator () {
  const ite = nanoiterator({ next: cb => cb(null, null) })
  ite.value = null
  return ite
}

function reduceIterators (iterators, sort) {
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
            updateValue(ite, left, right, sort)
            cb(null, ite.value)
          }
        }
      })

      updateValue(ite, left, right, sort)
      tmp.push(ite)
    }

    iterators = tmp
  }

  return iterators[0]
}

function updateValue (ite, left, right, sort) {
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
  constructor (storage, start, sort) {
    this.storage = storage
    this.values = []
    this.start = start
    this.end = start
    this.valueSize = 0
    this.size = 0
    this.sort = sort
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
    this.values.sort(this.sort)

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

function sortByKey (a, b) {
  if (a.key === b.key) return 0
  return a.key < b.key ? -1 : 1
}

function allocSpaces (n) {
  const buf = Buffer.allocUnsafe(n)
  buf.fill(32) // spaces
  return buf
}
