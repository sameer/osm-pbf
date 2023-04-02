/// The length of the BlobHeader should be less than 32 KiB (32*1024 bytes) and must be less than 64 KiB
const BLOB_HEADER_MAX_LEN: usize = 64 * 1024;

/// The uncompressed length of a Blob should be less than 16 MiB (16*1024*1024 bytes) and must be less than 32 MiB.
const BLOB_MAX_LEN: usize = 32 * 1024 * 1024;

/// Blob is a serialized [HeaderBlock]
const OSM_HEADER_TYPE: &str = "OSMHeader";

/// Blob is a serialized [PrimitiveBlock]
const OSM_DATA_TYPE: &str = "OSMData";

/// Protobuf code generated from the message definitions for PBF and OSM Data
///
/// <https://github.com/openstreetmap/OSM-binary/tree/master/osmpbf>
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::osmformat::{HeaderBlock, PrimitiveBlock};

/// Parse the PBF format into [FileBlock]s
pub mod parse;

/// Decode [FileBlock]s into [osm_types::Element]s
pub mod decode;

/// Serialize [FileBlock]s to the PBF format
pub mod serialize;

/// Fundamental unit of access in the PBF Format
#[derive(Debug, PartialEq, Clone)]
pub enum FileBlock {
    /// <https://wiki.openstreetmap.org/wiki/PBF_Format#Definition_of_the_OSMHeader_fileblock>
    Header(HeaderBlock),
    /// <https://wiki.openstreetmap.org/wiki/PBF_Format#Definition_of_OSMData_fileblock>
    Primitive(PrimitiveBlock),
    /// Unknown Block
    ///
    /// You can safely ignore these unless you are working with custom fileblocks.
    Other { r#type: String, bytes: Vec<u8> },
}

#[cfg(test)]
mod tests {
    use futures::{stream, StreamExt, TryStreamExt};
    use std::io::{Cursor, SeekFrom};
    use tokio::io::AsyncSeekExt;

    use crate::{
        decode::decode_elements, parse::get_osm_pbf_locations, parse::parse_osm_pbf,
        parse::parse_osm_pbf_from_locations, serialize::serialize_osm_pbf, serialize::Encoder,
    };

    /// <https://www.openstreetmap.org/api/0.6/map?bbox=-122.3199%2C47.4303%2C-122.2964%2C47.4647>
    ///
    /// ```bash
    /// wget 'https://www.openstreetmap.org/api/0.6/map?bbox=-122.3199%2C47.4303%2C-122.2964%2C47.4647' -O seatac.osm
    /// osmosis --read-xml seatac.osm --write-pbf seatac.osm.pbf
    /// ```
    const SEATAC_OSM_PBF: &[u8] = include_bytes!("../seatac.osm.pbf");

    #[tokio::test]
    async fn test_parse_osm_pbf() {
        let mut cursor = Cursor::new(SEATAC_OSM_PBF);
        let stream = parse_osm_pbf(&mut cursor);
        let blocks = stream.try_collect::<Vec<_>>().await.unwrap();

        // Go back to the beginning so we can parse again
        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let locations = get_osm_pbf_locations(&mut cursor)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let blocks_from_locations =
            parse_osm_pbf_from_locations(&mut cursor, stream::iter(locations))
                .try_collect::<Vec<_>>()
                .await
                .unwrap();

        assert_eq!(blocks.len(), 3);
        assert_eq!(blocks_from_locations.len(), 3);
        assert_eq!(blocks, blocks_from_locations);
    }

    #[tokio::test]
    async fn test_parse_serialize_parse_pbf() {
        let mut cursor = Cursor::new(SEATAC_OSM_PBF);
        let stream = parse_osm_pbf(&mut cursor);
        let blocks = stream.try_collect::<Vec<_>>().await.unwrap();

        for encoder in [
            None,
            #[cfg(feature = "zlib")]
            Some(Encoder::Zlib),
            #[cfg(feature = "zstd")]
            Some(Encoder::Zstd),
            #[cfg(feature = "lzma")]
            Some(Encoder::Lzma),
        ] {
            let mut temp = Cursor::new(vec![]);
            serialize_osm_pbf(stream::iter(blocks.clone()), &mut temp, encoder)
                .await
                .unwrap();

            let inner = temp.into_inner();

            let mut reparse_cursor = Cursor::new(inner);
            let reparse_stream = parse_osm_pbf(&mut reparse_cursor);
            let reparse_blocks = reparse_stream.try_collect::<Vec<_>>().await.unwrap();

            assert_eq!(blocks, reparse_blocks);
        }
    }

    #[tokio::test]
    async fn test_decode_elements() {
        let stream = parse_osm_pbf(Box::new(Cursor::new(SEATAC_OSM_PBF)));

        let element_count = decode_elements(stream.into_stream().map(|b| b.unwrap()))
            .try_fold(0, |acc, _| async move { Ok(acc + 1) })
            .await
            .unwrap();
        assert_eq!(element_count, 15232);
    }
}
