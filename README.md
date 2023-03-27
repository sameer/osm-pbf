# osm-pbf

Parse and write the [PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format) for [Open Street Map](https://www.openstreetmap.org/#map=13/47.4475/-122.3084) (OSM).

## Compression

There is a feature for each supported compression algorithm:

Name|Default Feature|Decode (read)|Encode (write)
---|---|---|---
Zlib|✅|✅|✅
Zstd|❌|✅|✅
Lzma|❌|✅|✅
Bzip2|❌|✅|❌
Lz4|❌|❌|❌

Bzip2 is deprecated so you cannot encode with that format.
Lz4 support is [not available yet](https://github.com/Nemo157/async-compression/issues/12).

There isn't any fine-grained control over encoding but feel free to file an issue if you are interested.

## Execution

This crate is written with async I/O for use with [tokio](https://tokio.rs/).

### Parallelism

The code is serial in nature but it's possible to parallelize encoding/decoding since fileblocks are independent in PBF.

Read parallelization example:

1. Call `get_osm_pbf_locations` to get a stream of fileblock locations.
1. Chunk the fileblocks into relevant groups. Namely: header followed by any number of primitive fileblocks.
1. Call `parse_osm_pbf_from_locations` for each chunk independently (i.e. [buffer_unordered](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffer_unordered)).
1. Process blocks as desired.

Write parallelization example:

1. Split your blocks into chunks of size N. Note: each chunk must contain a Header block followed by any number of Primitive blocks.
1. Call `write_osm_pbf` for each chunk independently (i.e. [buffer_unordered](https://docs.rs/futures/latest/futures/stream/trait.StreamExt.html#method.buffer_unordered)) with an in-memory vector as the writer.
1. As each call completes, write them to their final destination (i.e. a file).
