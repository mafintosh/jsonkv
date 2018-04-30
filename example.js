const jsonkv = require('./')
const from = require('from2')

const speed = require('speedometer')()
const db = jsonkv('db.json')

if (process.argv[2] === 'write') write()
else get()

function get () {
  db.get('hello', function (err, doc) {
    if (err) throw err
    console.log(doc)
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
