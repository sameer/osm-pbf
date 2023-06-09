# osm-pbf

Read and write the [PBF format](https://wiki.openstreetmap.org/wiki/PBF_Format) for [Open Street Map](https://www.openstreetmap.org/#map=13/47.4475/-122.3084) (OSM).

## Functionality

### Reading

There are two steps to reading the PBF format: parsing and decoding.
Parsing builds [fileblocks](https://wiki.openstreetmap.org/wiki/PBF_Format#Encoding_OSM_entities_into_fileblocks) out of the raw data.
Decoding converts fileblocks into the OSM [elements](https://wiki.openstreetmap.org/wiki/Elements) that they contain.

### Writing

Similarly, there are two steps to writing the PBF format: encoding and serialization.
Encoding converts OSM elements into fileblocks. This crate does not support encoding yet.
Serialization flattens fileblocks into raw data.

## Execution

This crate is written with async I/O for use with [tokio](https://tokio.rs/).

### Parallelism

The code is serial in nature but it's possible to parallelize encoding/decoding since fileblocks are independent in PBF.

Read parallelization example:

1. Call `get_osm_pbf_locations` to get a stream of fileblock locations
1. Call `parse_osm_pbf_at_location` for each location independently
1. Process blocks as desired

Write parallelization example:

1. Split your blocks into chunks
1. Call `write_osm_pbf` for each chunk independently with an in-memory vector as the writer
1. As each call completes, write them to their final destination (i.e. a file)

## Compression

There is a feature for each supported compression algorithm:

Name|Default Feature|Supported
---|---|---
Zlib|✅|✅
Zstd|❌|✅
Lzma|❌|✅
Lz4|❌|❌
Bzip2|❌|❌

Lz4 support is [not available yet](https://github.com/Nemo157/async-compression/issues/12).
Bzip2 has been deprecated for years so it is not supported.

There isn't any fine-grained control over encoding but feel free to file an issue if you are interested.
