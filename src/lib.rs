use std::{
    io::{Cursor, ErrorKind, SeekFrom},
    pin::Pin,
};

use async_compression::tokio::bufread::*;
use async_stream::try_stream;
use futures::{Stream, StreamExt, TryStream, TryStreamExt};
use protobuf::Message;
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
    fileformat::{blob::Data, Blob, BlobHeader},
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
    #[error("Failed to parse protobuf message: {0}")]
    Proto(#[from] protobuf::Error),
    #[error("BlobHeader is too long: {0} bytes")]
    BlobHeaderExceedsMaxLength(usize),
    #[error("Blob is too long: {0} bytes")]
    BlobExceedsMaxLength(usize),
    #[error("Blob is compressed with an unsupported algorithm: {0}")]
    UnsupportedCompression(&'static str),
}

/// Parse blob headers + blobs on the fly
fn stream_blobs<R: AsyncRead + Unpin>(
    pbf_reader: &mut R,
) -> impl TryStream<Ok = (BlobHeader, Blob), Error = ParseError> + Unpin + '_ {
    Box::pin(try_stream! {
        loop {
            let blob_header_len = if let Some(len) = read_blob_header_len(pbf_reader).await? { len } else { break; };

            let mut buf = vec![0; blob_header_len];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let header = BlobHeader::parse_from_bytes(buf.as_slice())?;
            let datasize = header.datasize() as usize;
            if datasize > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(datasize))?;
            }

            let mut buf = vec![0; datasize];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let body = Blob::parse_from_bytes(buf.as_slice())?;

            yield (header, body);
        }
    })
}

/// Same as [stream_blobs] but seeks past the blobs
fn stream_blob_headers<R: AsyncRead + Unpin + AsyncSeek>(
    pbf_reader: &mut R,
) -> impl TryStream<Ok = (BlobHeader, SeekFrom), Error = ParseError> + Unpin + '_ {
    Box::pin(try_stream! {
        let mut current_seek = 0;
        loop {
            let blob_header_len = if let Some(len) = read_blob_header_len(pbf_reader).await? { len } else { break; };
            // i32 = 4 bytes
            current_seek += 4;

            let mut buf = vec![0; blob_header_len];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let header = BlobHeader::parse_from_bytes(buf.as_slice())?;
            current_seek += blob_header_len;
            let datasize = header.datasize() as usize;
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
    pbf_reader: &mut R,
) -> Result<Option<usize>, ParseError> {
    match pbf_reader.read_i32().await.map(|len| len as usize) {
        Ok(size) if size > BLOB_HEADER_MAX_LEN => Err(ParseError::BlobHeaderExceedsMaxLength(size)),
        Ok(len) => Ok(Some(len)),
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Creates a stream for the decoded data from this block
fn decode_blob(blob: Blob) -> Result<Pin<Box<dyn AsyncRead>>, ParseError> {
    Ok(match blob.data.unwrap_or_else(|| Data::Raw(vec![])) {
        Data::Raw(raw) => Box::pin(Cursor::new(raw)),
        #[cfg(feature = "zlib")]
        Data::ZlibData(data) => Box::pin(ZlibDecoder::new(Cursor::new(data))),
        #[cfg(feature = "lzma")]
        Data::LzmaData(data) => Box::pin(LzmaDecoder::new(Cursor::new(data))),
        #[cfg(feature = "bzip2")]
        Data::OBSOLETEBzip2Data(data) => Box::pin(BzDecoder::new(Cursor::new(data))),
        #[cfg(feature = "zstd")]
        Data::ZstdData(data) => Box::pin(ZstdDecoder::new(Cursor::new(data))),
        other => {
            return Err(ParseError::UnsupportedCompression(match other {
                Data::Raw(_) => unreachable!(),
                Data::ZlibData(_) => "zlib",
                Data::LzmaData(_) => "lzma",
                Data::OBSOLETEBzip2Data(_) => "bzip2",
                Data::Lz4Data(_) => "lz4",
                Data::ZstdData(_) => "zstd",
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
            let blob_body_len = blob_body.raw_size() as usize;
            if blob_body_len > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(blob_body_len))?;
            }
            let mut buf = Vec::with_capacity(blob_body_len);
            let mut blob_stream = decode_blob(blob_body)?;
            blob_stream.read_to_end(&mut buf).await?;

            match blob_header.type_() {
                "OSMHeader" => {
                    yield FileBlock::Header(HeaderBlock::parse_from_bytes(buf.as_slice())?);
                },
                "OSMData" => {
                    yield FileBlock::Primitive(PrimitiveBlock::parse_from_bytes(buf.as_slice())?);
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
pub fn parse_osm_pbf<R: AsyncRead + Unpin>(
    pbf_reader: &mut R,
) -> impl TryStream<Ok = FileBlock, Error = ParseError> + Unpin + '_ {
    stream_osm_blocks(stream_blobs(pbf_reader))
}

/// Cursory examination of the data to get a stream of [FileBlockLocation]s
///
/// Use this in combination with [parse_osm_pbf_from_locations].
pub fn get_osm_pbf_locations<R: AsyncRead + AsyncSeek + Unpin>(
    pbf_reader: &mut R,
) -> impl TryStream<Ok = FileBlockLocation, Error = ParseError> + Unpin + '_ {
    Box::pin(try_stream! {
        let mut headers = stream_blob_headers(pbf_reader);

        while let Some((header, seek)) = headers.try_next().await? {
            yield FileBlockLocation { r#type: header.type_().to_string(), seek, len: header.datasize() as usize }
        }
    })
}

/// Parse the PBF format into a stream of [FileBlock]s
///
/// Use this in combination with [get_osm_pbf_locations].
pub fn parse_osm_pbf_from_locations<'a, R: AsyncRead + AsyncSeek + Unpin>(
    pbf_reader: &'a mut R,
    mut locations: impl Stream<Item = FileBlockLocation> + Unpin + 'a,
) -> impl TryStream<Ok = FileBlock, Error = ParseError> + Unpin + 'a {
    Box::pin(try_stream! {
        while let Some(location) = locations.next().await {
            pbf_reader.seek(location.seek).await?;

            let mut buf = vec![0; location.len];
            pbf_reader.read_exact(buf.as_mut()).await?;
            let blob_body = Blob::parse_from_bytes(buf.as_slice())?;

            let blob_body_len = blob_body.raw_size() as usize;
            if location.len > BLOB_MAX_LEN {
                Err(ParseError::BlobExceedsMaxLength(blob_body_len))?;
            }

            let mut buf = Vec::with_capacity(blob_body_len);
            let mut blob_stream = decode_blob(blob_body)?;
            blob_stream.read_to_end(&mut buf).await?;

            match location.r#type.as_str() {
                "OSMHeader" => {
                    yield FileBlock::Header(HeaderBlock::parse_from_bytes(buf.as_slice())?);
                },
                "OSMData" => {
                    yield FileBlock::Primitive(PrimitiveBlock::parse_from_bytes(buf.as_slice())?);
                }
                _ => {
                    yield FileBlock::Other { r#type: location.r#type, bytes: buf, }
                }
            }
        }
    })
}

/// Encoder to use when writing to a PBF file
///
/// This will only include encoders from enabled features.
/// Bzip2 is not an option since it is deprecated.
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
    #[error("Blob is compressed with an unsupported algorithm: {0}")]
    UnsupportedCompression(&'static str),
}

/// Applies the requested compression scheme, if any
fn encode<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    encoder: Option<Encoder>,
) -> Pin<Box<dyn AsyncRead + '_>> {
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
    out: &mut W,
    encoder: Option<Encoder>,
) -> tokio::io::Result<()> {
    while let Some(block) = blocks.next().await {
        let (raw_size, blob_bytes, type_) = match block {
            FileBlock::Header(header) => (
                header.compute_size(),
                header.write_to_bytes()?,
                OSM_HEADER_TYPE.to_string(),
            ),
            FileBlock::Primitive(primitive) => (
                primitive.compute_size(),
                primitive.write_to_bytes()?,
                OSM_DATA_TYPE.to_string(),
            ),
            FileBlock::Other { r#type, bytes } => (bytes.len() as u64, bytes, r#type),
        };

        let blob_encoded = {
            let mut cursor = Cursor::new(blob_bytes);
            let mut encoded = encode(&mut cursor, encoder);
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
            data: Some(match encoder {
                #[cfg(feature = "zlib")]
                Some(Encoder::Zlib) => Data::ZlibData(blob_encoded),
                #[cfg(feature = "zstd")]
                Some(Encoder::Zstd) => Data::ZstdData(blob_encoded),
                #[cfg(feature = "lzma")]
                Some(Encoder::Lzma) => Data::LzmaData(blob_encoded),
                None => Data::Raw(blob_encoded),
            }),
            ..Default::default()
        };

        let blob_header = BlobHeader {
            type_: Some(type_),
            indexdata: None,
            datasize: Some(blob.compute_size() as i32),
            ..Default::default()
        };

        out.write_i32(blob_header.compute_size() as i32).await?;
        out.write_all(&blob_header.write_to_bytes()?).await?;
        out.write_all(&blob.write_to_bytes()?).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use futures::{stream, TryStreamExt};
    use tokio::io::AsyncSeekExt;
    use std::io::{Cursor, SeekFrom};

    use crate::{
        get_osm_pbf_locations, parse_osm_pbf, parse_osm_pbf_from_locations, write_osm_pbf, Encoder,
    };

    const HONOLULU_OSM_PBF: &[u8] = include_bytes!("../honolulu_hawaii.osm.pbf");

    #[tokio::test]
    async fn test_parse_osm_pbf() {
        let mut cursor = Cursor::new(HONOLULU_OSM_PBF);
        let stream = parse_osm_pbf(&mut cursor);
        let blocks = stream.try_collect::<Vec<_>>().await.unwrap();

        // Go back to the beginning so we can parse again
        cursor.seek(SeekFrom::Start(0)).await.unwrap();
        let locations = get_osm_pbf_locations(&mut cursor)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let blocks_from_locations = parse_osm_pbf_from_locations(&mut cursor, stream::iter(locations))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(blocks.len(), 101);
        assert_eq!(blocks_from_locations.len(), 101);
        assert_eq!(blocks, blocks_from_locations);
    }

    #[tokio::test]
    async fn test_parse_write_parse_pbf() {
        let mut cursor = Cursor::new(HONOLULU_OSM_PBF);
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

            // Blob header length is the same
            assert_eq!(HONOLULU_OSM_PBF[..4], inner[..4]);

            let mut reparse_cursor = Cursor::new(inner);
            let reparse_stream = parse_osm_pbf(&mut reparse_cursor);
            let reparse_blocks = reparse_stream.try_collect::<Vec<_>>().await.unwrap();

            assert_eq!(blocks, reparse_blocks);
        }
    }
}
