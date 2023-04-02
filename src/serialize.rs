use std::{io::Cursor, pin::Pin};

use async_compression::tokio::bufread::*;
use futures::{Stream, StreamExt};
use quick_protobuf::{MessageWrite, Writer};
use thiserror::Error;
use tokio::io::{AsyncBufRead, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    protos::fileformat::{mod_Blob::OneOfdata as Data, Blob, BlobHeader},
    FileBlock, BLOB_HEADER_MAX_LEN, BLOB_MAX_LEN, OSM_DATA_TYPE, OSM_HEADER_TYPE,
};

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

/// Any error encountered in [serialize_osm_pbf]
#[derive(Error, Debug)]
pub enum SerializeError {
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

/// Serialize a stream of [FileBlock]s in the PBF format
pub async fn serialize_osm_pbf<W: AsyncWrite + Unpin>(
    mut blocks: impl Stream<Item = FileBlock> + Unpin,
    mut out: W,
    encoder: Option<Encoder>,
) -> Result<(), SerializeError> {
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
            Err(SerializeError::BlobExceedsMaxLength(raw_size))?;
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
            Err(SerializeError::BlobHeaderExceedsMaxLength(blob_header_size))?;
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
