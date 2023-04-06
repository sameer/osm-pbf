use std::{
    io::{Cursor, ErrorKind, SeekFrom},
    pin::Pin,
};

use crate::{
    protos::fileformat::{mod_Blob::OneOfdata as Data, Blob, BlobHeader},
    FileBlock, BLOB_HEADER_MAX_LEN, BLOB_MAX_LEN,
};
use async_compression::tokio::bufread::*;
use async_stream::try_stream;
use futures::{pin_mut, Stream, StreamExt,  TryStreamExt};
use quick_protobuf::{BytesReader, MessageRead};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncSeek, AsyncSeekExt};

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
fn stream_blobs<'a, R: AsyncRead + Unpin + Send + 'a>(
    mut pbf_reader: R,
) -> impl Stream<Item = Result<(BlobHeader, Blob), ParseError>> + Send + 'a {
    try_stream! {
        while let Some(blob_header_len) = read_blob_header_len(&mut pbf_reader).await? {
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
    }
}

/// Same as [stream_blobs] but seeks past the blobs
fn stream_blob_headers<'a, R: AsyncRead + Unpin + Send + AsyncSeek + 'a>(
    mut pbf_reader: R,
) -> impl Stream<Item = Result<(BlobHeader, SeekFrom), ParseError>> + Send + 'a {
    try_stream! {
        let mut current_seek = 0;
        while let Some(blob_header_len) = read_blob_header_len(&mut pbf_reader).await? {
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
    }
}

/// Convenience function for getting the length of the next blob header, if any remain
async fn read_blob_header_len<R: AsyncRead + Unpin + Send>(
    mut pbf_reader: R,
) -> Result<Option<usize>, ParseError> {
    match pbf_reader.read_i32().await.map(|len| len as usize) {
        Ok(len) if len > BLOB_HEADER_MAX_LEN => Err(ParseError::BlobHeaderExceedsMaxLength(len)),
        Ok(len) => Ok(Some(len)),
        Err(err) if err.kind() == ErrorKind::UnexpectedEof => Ok(None),
        Err(err) => Err(err.into()),
    }
}

/// Creates a stream for the decoded data from this block
fn decode_blob(blob: Blob) -> Result<Pin<Box<dyn AsyncRead + Send>>, ParseError> {
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
fn stream_osm_blocks<'a, R: AsyncRead + Unpin + Send + 'a>(
    pbf_reader: R,
) -> impl Stream<Item = Result<FileBlock, ParseError>> + Send + 'a {
    try_stream! {
        let blob_stream = stream_blobs(pbf_reader);
        pin_mut!(blob_stream);
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
    }
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
pub fn parse_osm_pbf<'a, R: AsyncRead + Unpin + Send + 'a>(
    pbf_reader: R,
) -> impl Stream<Item = Result<FileBlock, ParseError>> + Send + 'a {
    stream_osm_blocks(pbf_reader)
}

/// Cursory examination of the data to get a stream of [FileBlockLocation]s
///
/// Use this in combination with [parse_osm_pbf_from_locations] for reading in parallel.
pub fn get_osm_pbf_locations<'a, R: AsyncRead + AsyncSeek + Unpin + Send + 'a>(
    pbf_reader: R,
) -> impl Stream<Item = Result<FileBlockLocation, ParseError>> + Send + 'a {
    try_stream! {
        let headers = stream_blob_headers(pbf_reader);
        pin_mut!(headers);

        while let Some((header, seek)) = headers.try_next().await? {
            yield FileBlockLocation { r#type: header.type_pb, seek, len: header.datasize as usize }
        }
    }
}

/// Parse the PBF format into a stream of [FileBlock]s
///
/// Use this in combination with [get_osm_pbf_locations] for reading in parallel.
pub fn parse_osm_pbf_from_locations<'a, R: AsyncRead + AsyncSeek + Unpin + Send + 'a>(
    mut pbf_reader: R,
    mut locations: impl Stream<Item = FileBlockLocation> + Unpin + Send + 'a,
) -> impl Stream<Item = Result<FileBlock, ParseError>> + Send + 'a {
    try_stream! {
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
    }
}

/// [quick_protobuf::reader::deserialize_from_slice] but doesn't read length
fn deserialize_from_slice<'a, M: MessageRead<'a>>(
    bytes: &'a [u8],
) -> Result<M, quick_protobuf::Error> {
    let mut reader = BytesReader::from_bytes(bytes);
    reader.read_message_by_len(bytes, bytes.len())
}
