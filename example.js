const jsonkv = require('./')
const from = require('from2')

const db = jsonkv('db.json')

if (process.argv[2] === 'write') write()
else get()

function get () {
  db.open(function (err, doc) { // we don't need to open but do so to avoid timing it
    if (err) throw err
    const then = process.hrtime()
    db.get('hello', function (err, doc) {
      if (err) throw err
      const delta = process.hrtime(then)
      console.log(delta, doc)
    })
  })
}

function write () {
  var len = 100000000
  const target = Math.floor(Math.random() * len)
  const stream = jsonkv.createWriteStream('db.json')

  const rs = from.obj(function (size, cb) {
    if (!len) return cb(null, null)

    const data = {
      key: len === target
        ? 'hello'
        : Math.random().toString(16).slice(2),
      value: len--
    }

    cb(null, data)
  })

  rs.pipe(stream)
}
