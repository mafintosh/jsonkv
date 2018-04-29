# jsonkv

Single file write-once database that is valid JSON with efficient random access on bigger datasets

```
npm install jsonkv
```

(WIP)

## Usage

``` js
const jsonkv = require('jsonkv')

// First create a database (all data will be stored in ./db.json as valid JSON)
const ws = jsonkv.createWriteStream('db.json')

// Write a ton of data to it
for (var i = 0; i < 10000; i++) {
  ws.write({
    key: i,
    value: `this is a value: ${i}`
  })
}

ws.end(function () {
  // our jsonkv is now fully written and cannot be updated again.
  // to query it make an instance
  const db = jsonkv('db.json')

  db.get(42, function (err, doc) {
    console.log(doc) // prints {key: 42, value: 'this is a value: 42'}
  })
})
```

## License

MIT
