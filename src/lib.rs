use std::{
    io::{Cursor, ErrorKind, SeekFrom},
    pin::Pin,
};

use async_compression::tokio::bufread::*;
use async_stream::try_stream;
use futures::{Stream, StreamExt, TryStream, TryStreamExt};
use quick_protobuf::{BytesReader, MessageRead, MessageWrite, Writer};
use thiserror::Error;
use tokio::io::{
    AsyncBufRead, AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt, AsyncWrite, AsyncWriteExt,
};

/// Protobuf code generated from the message definitions for PBF and OSM Data
///
/// <https://github.com/openstreetmap/OSM-binary/tree/master/osmpbf>
pub mod protos {
    include!(concat!(env!("OUT_DIR"), "/protos/mod.rs"));
}

use protos::{
    fileformat::{mod_Blob::OneOfdata as Data, Blob, BlobHeader},
    osmformat::{HeaderBlock, PrimitiveBlock},
};

/// The length of the BlobHeader should be less than 32 KiB (32*1024 bytes) and must be less than 64 KiB
const BLOB_HEADER_MAX_LEN: usize = 64 * 1024;

/// The uncompressed length of a Blob should be less than 16 MiB (16*1024*1024 bytes) and must be less than 32 MiB.
const BLOB_MAX_LEN: usize = 32 * 1024 * 1024;

/// Blob is a serialized [HeaderBlock]
const OSM_HEADER_TYPE: &str = "OSMHeader";

/// Blob is a serialized [PrimitiveBlock]
const OSM_DATA_TYPE: &str = "OSMData";

/// Any error encountered in [parse_osm_pbf]
#[derive(Error, Debug)]
pub enum ParseError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error("Failed to deserialize protobuf message: {0}")]
    Proto(#[from] quick_protobuf::Error),
    #[error("BlobHeader is too long: {0} bytes")]
    BlobHeaderExceedsMaxLength(usize),
    #[error("Blob is too long: {0} bytes")]
    BlobExceedsMaxLength(usize),
    #[error("Blob is compressed with an unsupported algorithm: {0}")]
    UnsupportedCompression(&'static str),
}

/// Parse blob headers + blobs on the fly
fn stream_blobs<R: AsyncRead + Unpin>(
    mut pbf_reader: R,
) -> impl TryStream<Ok = (BlobHeader, Blob), Error = ParseError> + Unpin {
    Box::pin(try_stream! {
        loop {
            let blob_header_len = if let Some(len) = read_blob_header_len(&mut pbf_reader).await? { len } else { break; };
            let mut buf = vec![0; blob_header_len];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let header: BlobHeader = deserialize_from_slice(buf.as_slice())?;

            let datasize = header.datasize as usize;
            if datasize > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(datasize))?;
            }

            let mut buf = vec![0; datasize];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let body: Blob = deserialize_from_slice(buf.as_slice())?;

            yield (header, body);
        }
    })
}

/// Same as [stream_blobs] but seeks past the blobs
fn stream_blob_headers<R: AsyncRead + Unpin + AsyncSeek>(
    mut pbf_reader: R,
) -> impl TryStream<Ok = (BlobHeader, SeekFrom), Error = ParseError> + Unpin {
    Box::pin(try_stream! {
        let mut current_seek = 0;
        loop {
            let blob_header_len = if let Some(len) = read_blob_header_len(&mut pbf_reader).await? { len } else { break; };
            // i32 = 4 bytes
            current_seek += 4;

            let mut buf = vec![0; blob_header_len];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let header: BlobHeader = deserialize_from_slice(buf.as_slice())?;
            current_seek += blob_header_len;
            let datasize = header.datasize as usize;
            if datasize > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(datasize))?;
            }

            pbf_reader.seek(SeekFrom::Current(datasize as i64)).await?;

            yield (header, SeekFrom::Start(current_seek as u64));

            current_seek += datasize;
        }
    })
}

/// Convenience function for getting the length of the next blob header, if any remain
async fn read_blob_header_len<R: AsyncRead + Unpin>(
    mut pbf_reader: R,
) -> Result<Option<usize>, ParseError> {
    match pbf_reader.read_i32().await.map(|len| len as usize) {
        Ok(size) if size > BLOB_HEADER_MAX_LEN => Err(ParseError::BlobHeaderExceedsMaxLength(size)),
        Ok(len) => Ok(Some(len)),
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Creates a stream for the decoded data from this block
///
/// [quick_protobuf::reader::deserialize_from_slice] but doesn't read length
fn decode_blob(blob: Blob) -> Result<Pin<Box<dyn AsyncRead>>, ParseError> {
    Ok(match blob.data {
        Data::raw(raw) => Box::pin(Cursor::new(raw)),
        #[cfg(feature = "zlib")]
        Data::zlib_data(data) => Box::pin(ZlibDecoder::new(Cursor::new(data))),
        #[cfg(feature = "lzma")]
        Data::lzma_data(data) => Box::pin(LzmaDecoder::new(Cursor::new(data))),
        #[cfg(feature = "zstd")]
        Data::zstd_data(data) => Box::pin(ZstdDecoder::new(Cursor::new(data))),
        Data::None => Box::pin(Cursor::new(vec![])),
        other => {
            return Err(ParseError::UnsupportedCompression(match other {
                Data::raw(_) | Data::None => unreachable!(),
                Data::zlib_data(_) => "zlib",
                Data::lzma_data(_) => "lzma",
                Data::lz4_data(_) => "lz4",
                Data::zstd_data(_) => "zstd",
            }))
        }
    })
}

/// Blobs to blocks
fn stream_osm_blocks(
    mut blob_stream: impl TryStream<Ok = (BlobHeader, Blob), Error = ParseError> + Unpin,
) -> impl TryStream<Ok = FileBlock, Error = ParseError> + Unpin {
    Box::pin(try_stream! {
        while let Some((blob_header, blob_body)) = blob_stream.try_next().await? {
            let blob_body_len = blob_body.raw_size.unwrap_or_default() as usize;
            if blob_body_len > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(blob_body_len))?;
            }
            let mut buf = Vec::with_capacity(blob_body_len);
            let mut blob_stream = decode_blob(blob_body)?;
            blob_stream.read_to_end(&mut buf).await?;

            match blob_header.type_pb.as_str() {
                "OSMHeader" => {
                    yield FileBlock::Header(deserialize_from_slice(buf.as_slice())?);
                },
                "OSMData" => {
                    yield FileBlock::Primitive(deserialize_from_slice(buf.as_slice())?);
                }
                other => {
                    yield FileBlock::Other { r#type: other.to_string(), bytes: buf, }
                }
            }
        }
    })
}

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

/// Location of a block in an [AsyncSeek]
///
/// This struct is used in a parallelized read workflow.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct FileBlockLocation {
    /// Type of the block
    pub r#type: String,

    /// Absolute location of this block for use with an [AsyncSeek]
    pub seek: SeekFrom,

    /// Bytes in the [AsyncRead] to read for this block
    pub len: usize,
}

/// Parse the PBF format into a stream of [FileBlock]s
pub fn parse_osm_pbf<'a, R: AsyncRead + Unpin + 'a>(
    pbf_reader: R,
) -> impl TryStream<Ok = FileBlock, Error = ParseError> + Unpin + 'a {
    stream_osm_blocks(stream_blobs(pbf_reader))
}

/// Cursory examination of the data to get a stream of [FileBlockLocation]s
///
/// Use this in combination with [parse_osm_pbf_from_locations] for reading in parallel.
pub fn get_osm_pbf_locations<'a, R: AsyncRead + AsyncSeek + Unpin + 'a>(
    pbf_reader: R,
) -> impl TryStream<Ok = FileBlockLocation, Error = ParseError> + Unpin + 'a {
    Box::pin(try_stream! {
        let mut headers = stream_blob_headers(pbf_reader);

        while let Some((header, seek)) = headers.try_next().await? {
            yield FileBlockLocation { r#type: header.type_pb, seek, len: header.datasize as usize }
        }
    })
}

/// Parse the PBF format into a stream of [FileBlock]s
///
/// Use this in combination with [get_osm_pbf_locations] for reading in parallel.
pub fn parse_osm_pbf_from_locations<'a, R: AsyncRead + AsyncSeek + Unpin + 'a>(
    mut pbf_reader: R,
    mut locations: impl Stream<Item = FileBlockLocation> + Unpin + 'a,
) -> impl TryStream<Ok = FileBlock, Error = ParseError> + Unpin + 'a {
    Box::pin(try_stream! {
        while let Some(location) = locations.next().await {
            pbf_reader.seek(location.seek).await?;

            let mut buf = vec![0; location.len];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let blob_body: Blob = deserialize_from_slice(buf.as_slice())?;

            let blob_body_len = blob_body.raw_size.unwrap_or_default() as usize;
            if location.len > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(blob_body_len))?;
            }

            let mut buf = Vec::with_capacity(blob_body_len);
            let mut blob_stream = decode_blob(blob_body)?;
            blob_stream.read_to_end(&mut buf).await?;

            match location.r#type.as_str() {
                "OSMHeader" => {
                    yield FileBlock::Header(deserialize_from_slice(buf.as_slice())?);
                },
                "OSMData" => {
                    yield FileBlock::Primitive(deserialize_from_slice(buf.as_slice())?);
                }
                _ => {
                    yield FileBlock::Other { r#type: location.r#type, bytes: buf, }
                }
            }
        }
    })
}

fn deserialize_from_slice<'a, M: MessageRead<'a>>(
    bytes: &'a [u8],
) -> Result<M, quick_protobuf::Error> {
    let mut reader = BytesReader::from_bytes(bytes);
    reader.read_message_by_len(bytes, bytes.len())
}

/// Encoder to use when writing to a PBF file
///
/// This will only include encoders from enabled features.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Encoder {
    #[cfg(feature = "zlib")]
    Zlib,
    #[cfg(feature = "zstd")]
    Zstd,
    #[cfg(feature = "lzma")]
    Lzma,
}

/// Any error encountered in [write_osm_pbf]
#[derive(Error, Debug)]
pub enum WriteError {
    #[error(transparent)]
    Io(#[from] tokio::io::Error),
    #[error("BlobHeader is too long: {0} bytes")]
    BlobHeaderExceedsMaxLength(usize),
    #[error("Blob is too long: {0} bytes")]
    BlobExceedsMaxLength(usize),
    #[error("Failed to serialize protobuf message: {0}")]
    Proto(#[from] quick_protobuf::Error),
}

/// Applies the requested compression scheme, if any
fn encode<R: AsyncBufRead + Unpin + 'static>(
    reader: R,
    encoder: Option<Encoder>,
) -> Pin<Box<dyn AsyncRead>> {
    if let Some(encoder) = encoder {
        match encoder {
            #[cfg(feature = "zlib")]
            Encoder::Zlib => Box::pin(ZlibEncoder::new(reader)),
            #[cfg(feature = "zstd")]
            Encoder::Zstd => Box::pin(ZstdEncoder::new(reader)),
            #[cfg(feature = "lzma")]
            Encoder::Lzma => Box::pin(LzmaEncoder::new(reader)),
        }
    } else {
        Box::pin(reader)
    }
}

/// Write a stream of [FileBlock]s in the PBF format
pub async fn write_osm_pbf<W: AsyncWrite + Unpin>(
    mut blocks: impl Stream<Item = FileBlock> + Unpin,
    mut out: W,
    encoder: Option<Encoder>,
) -> Result<(), WriteError> {
    while let Some(block) = blocks.next().await {
        let (raw_size, blob_bytes, type_pb) = match block {
            FileBlock::Header(header) => (
                header.get_size(),
                serialize_into_vec(&header)?,
                OSM_HEADER_TYPE.to_string(),
            ),
            FileBlock::Primitive(primitive) => (
                primitive.get_size(),
                serialize_into_vec(&primitive)?,
                OSM_DATA_TYPE.to_string(),
            ),
            FileBlock::Other { r#type, bytes } => (bytes.len(), bytes, r#type),
        };

        if raw_size > BLOB_MAX_LEN {
            Err(WriteError::BlobExceedsMaxLength(raw_size))?;
        }

        let blob_encoded = {
            let cursor = Cursor::new(blob_bytes);
            let mut encoded = encode(cursor, encoder);
            let mut buf = vec![];
            encoded.read_to_end(&mut buf).await?;
            buf
        };

        let blob = Blob {
            raw_size: if encoder.is_some() {
                Some(raw_size as i32)
            } else {
                None
            },
            data: match encoder {
                #[cfg(feature = "zlib")]
                Some(Encoder::Zlib) => Data::zlib_data(blob_encoded),
                #[cfg(feature = "zstd")]
                Some(Encoder::Zstd) => Data::zstd_data(blob_encoded),
                #[cfg(feature = "lzma")]
                Some(Encoder::Lzma) => Data::lzma_data(blob_encoded),
                None => Data::raw(blob_encoded),
            },
        };

        let blob_header = BlobHeader {
            type_pb,
            indexdata: None,
            datasize: blob.get_size() as i32,
        };

        let blob_header_size = blob_header.get_size();
        if blob_header_size > BLOB_HEADER_MAX_LEN {
            Err(WriteError::BlobHeaderExceedsMaxLength(blob_header_size))?;
        }

        out.write_i32(blob_header_size as i32).await?;
        out.write_all(&serialize_into_vec(&blob_header)?).await?;
        out.write_all(&serialize_into_vec(&blob)?).await?;
    }
    Ok(())
}

/// [quick_protobuf::writer::serialize_into_vec] but doesn't write length
fn serialize_into_vec<M: MessageWrite>(message: &M) -> Result<Vec<u8>, quick_protobuf::Error> {
    let len = message.get_size();
    let mut v = Vec::with_capacity(len);

    {
        let mut writer = Writer::new(&mut v);
        message.write_message(&mut writer)?;
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use futures::{stream, TryStreamExt};
    use std::io::{Cursor, SeekFrom};
    use tokio::io::AsyncSeekExt;

    use crate::{
        get_osm_pbf_locations, parse_osm_pbf, parse_osm_pbf_from_locations, write_osm_pbf, Encoder,
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
    async fn test_parse_write_parse_pbf() {
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
            write_osm_pbf(stream::iter(blocks.clone()), &mut temp, encoder)
                .await
                .unwrap();

            let inner = temp.into_inner();

            let mut reparse_cursor = Cursor::new(inner);
            let reparse_stream = parse_osm_pbf(&mut reparse_cursor);
            let reparse_blocks = reparse_stream.try_collect::<Vec<_>>().await.unwrap();

            assert_eq!(blocks, reparse_blocks);
        }
    }
}
