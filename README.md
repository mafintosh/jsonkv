# wodb

Single file Write-Once Database with efficient random access on bigger datasets

```
npm install wodb
```

(WIP)

## Usage

``` js
const wodb = require('wodb')

// First create a database (all data will be stored in ./my.db as JSON)
const ws = wodb.createWriteStream('my.db')

// Write a ton of data to it
for (var i = 0; i < 10000; i++) {
  ws.write({
    key: i,
    value: `this is a value: ${i}`
  })
}

ws.end(function () {
  // our wodb is now fully written and cannot be updated again.
  // to query it make an instance
  const db = wodb('my.db')

  db.get(42, function (err, doc) {
    console.log(doc) // prints {key: 42, value: 'this is a value: 42'}
  })
})
```

## License

MIT
