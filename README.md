# jsonkv

Single file write-once database that is valid JSON with efficient random access on bigger datasets

```
npm install jsonkv
```

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

## API

#### `ws = jsonkv.createWriteStream(filename, [opts])`

Create a new database by writing data to the input stream.

All data should be objects and include a sortable primary key. Per default the property `key` is used. If you want to use another property pass your own sort function as in options.

```
const ws = jsonkv.createWriteStream('db.json')

ws.write({
  key: 'hello', // per default key is used as the primary key
  world: true
})
```

The data will be stored temporarily as `{filename}.tmp` and will then be indexed and stored in `filename` as a valid JSON file where all the data is stored sorted in a `values` array with some whitespace padding to make lookups efficient.

The indexing procedure is memory efficient so should be able to handle large datasets as input.

When the stream emits `finish` the database is safe to use.

#### `db = jsonkv(filename, [opts])`

After writing data to a database file you can query by making a database instance.

If you used an optional sort function when writing your data you should pass that here as well.

#### `db.get(key, callback)`

Lookup a key. Return the value if found and `null` otherwise.

#### `rs = db.createReadStream([opts])`

Make a readable stream that traverses the database in sorted order.

Options include:

``` js
{
  gt: key,  // only keys > than key
  gte: key, // only keys >= than key
  lt: key,  // only keys < than key
  lte: key  // only keys <= than key
}
```

#### `ite = db.iterate([opts])`

Same as above but returns a [nanoiterator](https://github.com/mafintosh/nanoiterator) instance instead of a stream

## License

MIT
