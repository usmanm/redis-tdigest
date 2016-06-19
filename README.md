# redis-tdigest

[![Build Status](https://img.shields.io/circleci/project/usmanm/redis-tdigest.svg?style=flat-square)](https://circleci.com/gh/usmanm/redis-tdigest)

This is a Redis module for the [t-digest](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) data structure which can be used for accurate online accumulation of rank-based statistics such as quantiles and cumulative distribution at a point. The implementation is based on the [Merging Digest](https://github.com/tdunning/t-digest/blob/master/src/main/java/com/tdunning/math/stats/MergingDigest.java) implementation by the author.

## Building & Loading

Before going ahead, make sure that the Redis server you're using has support for [Redis modules](http://antirez.com/news/106).

First, you'll have to build the Redis t-digest module from source.

```
make
```

This should generate a shared library called `tdigest.so` in the root folder. You can now load it into Redis by using the following `redis.conf` configuration directive:

```
loadmodule /path/to/tdigest.so
```

Alternatively, you can load it on an already running Redis server by issuing the following commands:

```
MODULE LOAD /path/to/tdigest.so
```

## API

#### `TDIGEST.NEW key [compression]`

Initializes a `key` to an empty t-digest structure with the `compression` provided or with the default compression of `400`.

*Reply:* `"OK"`

#### `TDIGEST.ADD key value count [value count ...]`

Adds a `value` with the specified `count`. If key is is missing, an empty t-digest structure is initialized with a default compression of `400`. Returns the sum of counts for all values added.

*Reply:* `long long`

#### `TDIGEST.MERGE destkey sourcekey [sourcekey ...]`

Merges one or more `sourcekey` into `destkey`. If `destkey` is missing, an empty t-digest structure is initialized with a default compression of `400`.

*Reply:* `"OK"`

#### `TDIGEST.CDF key value [value ...]`

Returns the cumulative distribution for all provided values. `value` must be a double. The cumulative distribution returned for all values is between `0..1`.

*Reply:* `double` array or `nil` if key missing

#### `TDIGEST.QUANTILE key quantile [quantile ...]`

Returns the estimate values at all provided quantiles. `quantile` must be a `double` between `0..1`.

*Reply:* `double` array or `nil` if key missing

#### `TDIGEST.DEBUG key`

Prints debug information about the t-digest.

*Reply:* bulk strings array

The reply is of the form:

```
1) TDIGEST (<compression>, <num_centroids>, <memory size>)
2)   CENTROID (<mean>, <weight>)
3)   CENTROID (<mean>, <weight>)
4)   CENTROID (<mean>, <weight>)
5)   ...
```

Centroids are printed in sorted order with respect to their mean.

## Testing

The integration tests require a running Redis server so you must have `redis-server` on your `PATH` or pass its location in an environment variable called `REDIS_SERVER`. Tests are written in Python and use the [pytest](http://pytest.org/latest/) unit testing library.

```
make test
```

## Contributing

Bug reports, feature and pull requests are welcome! Please add tests for any non-trivial changes you submit.
