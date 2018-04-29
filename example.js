const createWriteStream = require('./write-stream')

const stream = createWriteStream('db.json')

for (var i = 0; i < 1000000; i++) {
  stream.write({
    key: Math.random().toString(16).slice(2),
    value: i
  })
}

stream.end()
