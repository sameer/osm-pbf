use std::{collections::HashMap, string::FromUtf8Error};

use crate::{
    protos::osmformat::{
        mod_Relation::MemberType as MemberTypePbf, Info as InfoPbf, PrimitiveBlock, StringTable,
    },
    FileBlock,
};
use async_stream::try_stream;
use chrono::NaiveDateTime;
use futures::{Stream, StreamExt, TryStream};
use itertools::multizip;
use num_traits::CheckedAdd;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use thiserror::Error;

use osm_types::{Element, Id, Info, Member, MemberType, Node, Relation, Way};

/// Any error encountered in [decode_elements]
#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("String in string table is not UTF8: {0}")]
    StringTable(#[from] FromUtf8Error),
    #[error("Relation element has an unknown member type: {0}")]
    UnknownRelationMemberType(i32),
    #[error("Addition overflow while decoding delta-encoded values")]
    DeltaOverflow,
    #[error("Failed to calculate node position: {0}")]
    Position(#[from] rust_decimal::Error),
}

/// Decode a PBF [FileBlock] stream into an [Element] stream
///
/// There is no dependency on order, so this can be called in parallel.
/// Technically, the file format is `(<HeaderBlock> (<PrimitiveBlock>)+)*`, but
/// [FileBlock::Header] and [FileBlock::Other] are ignored since we are only
/// interested in elements.
///
///
/// A number of hydration steps are performed:
/// * String table indices ==> actual strings
/// * Positions packed as granularity, offset, value ==> positions with [Decimal]
/// * Timestamps packed as date_granularity, value ==> timestamps with [NaiveDateTime]
/// * Dense nodes => [Node]s
/// * Delta-encoded values => actual values
///
/// <https://wiki.openstreetmap.org/wiki/PBF_Format#Encoding_OSM_entities_into_fileblocks>
pub fn decode_elements(
    mut block_stream: impl Stream<Item = FileBlock> + Unpin,
) -> impl TryStream<Ok = Element, Error = DecodeError> + Unpin {
    Box::pin(try_stream! {
        while let Some(block) = block_stream.next().await {
            match block {
                FileBlock::Primitive(PrimitiveBlock {
                    stringtable: StringTable { s: raw_string_table, },
                    primitivegroup,
                    granularity,
                    lat_offset,
                    lon_offset,
                    date_granularity,
                }) => {
                    let mut string_table = Vec::with_capacity(raw_string_table.len());
                    for raw_string in raw_string_table {
                        string_table.push(String::from_utf8(raw_string)?);
                    }

                    for group in primitivegroup {
                        for node in group.nodes {
                            yield Element::Node(Node {
                                id: Id(node.id),
                                attributes: kv_to_attributes(&node.keys, &node.vals, &string_table),
                                info: node.info.as_ref().map(|info| info_from_pbf(info, date_granularity, &string_table)),
                                lat: packed_to_degrees(node.lat, granularity, lat_offset)?,
                                lon: packed_to_degrees(node.lon, granularity, lon_offset)?,
                            });
                        }
                        if let Some(dense) = group.dense {
                            let (mut dense_node_it, mut dense_attrs_it) = {
                                let id_it = Delta::from(dense.id.iter().copied());
                                let lat_it = Delta::from(dense.lat.iter().copied());
                                let lon_it = Delta::from(dense.lon.iter().copied());
                                let dense_attrs = dense_kv_to_attributes(&dense.keys_vals, dense.id.len(), &string_table);
                                #[cfg(debug)]
                                {
                                    assert_eq!(dense.id.len(), dense.lat.len());
                                    assert_eq!(dense.lat.len(), dense.lon.len());
                                    if !dense_attrs.is_empty() {
                                        assert_eq!(dense.lon.len(), dense_attrs.len());
                                    }
                                }
                                (multizip((id_it, lat_it, lon_it)), dense_attrs.into_iter())
                            };

                            let mut dense_info_it = if let Some(dense_info) = dense.denseinfo.as_ref() {
                                let version_it = dense_info.version.iter().copied();
                                let timestamp_it = Delta::from(dense_info.timestamp.iter().copied());
                                let changeset_it = Delta::from(dense_info.changeset.iter().copied());
                                let uid_it = Delta::from(dense_info.uid.iter().copied());
                                let user_sid_it = Delta::from(dense_info.user_sid.iter().copied());
                                let visible_it = dense_info.visible.iter().copied();
                                #[cfg(debug)]
                                {
                                    assert_eq!(dense_info.version.len(), dense_info.timestamp.len());
                                    assert_eq!(dense_info.timestamp.len(), dense_info.changeset.len());
                                    assert_eq!(dense_info.changeset.len(), dense_info.uid.len());
                                    assert_eq!(dense_info.uid.len(), dense_info.user_sid.len());
                                    if !dense_info.visible.is_empty() {
                                        assert_eq!(dense_info.user_sid.len(), dense_info.visible.len());
                                    }
                                }
                                Some((multizip((version_it, timestamp_it, changeset_it, uid_it, user_sid_it)), visible_it))
                            } else { None };

                            while let (Some((id, lat, lon)), attributes) = (dense_node_it.next(), dense_attrs_it.next()) {
                                yield Element::Node(Node {
                                    id: Id(id?),
                                    attributes: attributes.unwrap_or_default(),
                                    info: if let Some(((version, timestamp, changeset, uid, user_sid), visible)) = dense_info_it.as_mut().and_then(|(it, visible_it)| it.next().zip(Some(visible_it.next()))) {
                                        Some(info_from_pbf(&InfoPbf {
                                            version,
                                            timestamp: Some(timestamp?),
                                            changeset: Some(changeset?),
                                            uid: Some(uid?),
                                            // OSM authors indicated that this is incorrectly specified in the proto file
                                            // so it is ok to convert like this.
                                            user_sid: Some(user_sid? as u32),
                                            visible,
                                        }, date_granularity, &string_table))
                                    } else { None },
                                    lat: packed_to_degrees(lat?, granularity, lat_offset)?,
                                    lon: packed_to_degrees(lon?, granularity, lon_offset)?,
                                });
                            }
                        }
                        for way in group.ways {
                            #[cfg(debug)]
                            {
                                assert_eq!(way.lat.len(), way.lon.len());
                                if !lat.is_empty() {
                                    assert_eq!(way.refs.len(), way.lat.len());
                                }
                            }
                            yield Element::Way(Way {
                                id: Id(way.id),
                                attributes: kv_to_attributes(&way.keys, &way.vals, &string_table),
                                info: way.info.as_ref().map(|info| info_from_pbf(info, date_granularity, &string_table)),
                                refs: {
                                    let mut refs = Vec::with_capacity(way.refs.len());
                                    let delta = Delta::from(way.refs.into_iter());
                                    for r in delta {
                                        refs.push(Id(r?));
                                    }

                                    refs
                                },
                            })
                        }
                        for relation in group.relations {
                            #[cfg(debug)]
                            {
                                assert_eq!(relation.roles_sid.len(), relation.memids.len());
                                assert_eq!(relation.memids.len(), relation.types.len());
                            }
                            let member_it = multizip((relation.roles_sid.iter().copied(), relation.memids.iter().copied(), relation.types.iter().copied()));
                            let mut members = Vec::with_capacity(relation.roles_sid.len());
                            for (role_sid, member_id, ty) in member_it {
                                members.push(Member {
                                    id: Id(member_id),
                                    ty: ty.into(),
                                    role: string_table.get(role_sid as usize).filter(|s| !s.is_empty()).cloned(),
                                });
                            }

                            yield Element::Relation(Relation {
                                id: Id(relation.id),
                                attributes: kv_to_attributes(&relation.keys, &relation.vals, &string_table),
                                info: relation.info.as_ref().map(|info| info_from_pbf(info, date_granularity, &string_table)),
                                members,
                            })
                        }
                    }
                },
                FileBlock::Header(_) | FileBlock::Other { .. } => {},
            }
        }
    })
}

/// Converts key & value index arrays into an attribute map
fn kv_to_attributes(
    keys: &[u32],
    vals: &[u32],
    string_table: &[String],
) -> HashMap<String, String> {
    #[cfg(debug)]
    assert_eq!(keys.len(), vals.len());

    keys.iter()
        .copied()
        .zip(vals.iter().copied())
        .filter_map(|(k, v)| {
            string_table
                .get(k as usize)
                .zip(string_table.get(v as usize))
                .filter(|(k, _v)| !k.is_empty())
                .map(|(k, v)| (k.clone(), v.clone()))
        })
        .collect()
}

/// Converts a dense key-value array into attribute maps
///
/// This differs from [kv_to_attributes] because the keys and values
/// for _multiple_ nodes are packed into a single array and end-delimited by a `0`.
///
/// <https://wiki.openstreetmap.org/wiki/PBF_Format#Nodes>
fn dense_kv_to_attributes(
    keys_vals: &[i32],
    size_hint: usize,
    string_table: &[String],
) -> Vec<HashMap<String, String>> {
    // Nothing to unpack
    if keys_vals.is_empty() {
        return vec![];
    }

    let mut acc: Vec<HashMap<_, _>> = Vec::with_capacity(size_hint);

    let mut current = HashMap::new();
    let mut key = None;
    for &k_or_v in keys_vals {
        if k_or_v == 0 {
            acc.push(current);
            current = HashMap::new();
        } else if let Some(key) = key.take() {
            current.insert(
                key,
                string_table
                    .get(k_or_v as usize)
                    .cloned()
                    .unwrap_or_default(),
            );
        } else {
            key = Some(
                string_table
                    .get(k_or_v as usize)
                    .cloned()
                    .unwrap_or_default(),
            );
        }
    }
    #[cfg(debug)]
    assert_eq!(key, None);

    acc
}

/// Converts an iterator into a cumulative sum iterator with checks for overflow
///
/// Used to decode delta-encoded PBF values.
struct Delta<T, I>
where
    I: Iterator<Item = T>,
{
    acc: Option<T>,
    iterator: I,
}

impl<T, I> From<I> for Delta<T, I>
where
    I: Iterator<Item = T>,
{
    fn from(iterator: I) -> Self {
        Self {
            acc: None,
            iterator,
        }
    }
}

impl<T, I> Iterator for Delta<T, I>
where
    T: Clone + CheckedAdd,
    I: Iterator<Item = T>,
{
    type Item = Result<T, DecodeError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.iterator.next() {
            Some(x) => {
                match self.acc.as_mut() {
                    Some(acc) => match acc.checked_add(&x) {
                        Some(new_acc) => {
                            *acc = new_acc;
                        }
                        None => return Some(Err(DecodeError::DeltaOverflow)),
                    },
                    None => {
                        self.acc = Some(x);
                    }
                };

                self.acc.clone().map(Ok)
            }
            None => None,
        }
    }
}

/// Calculates the latitude/longitude degrees from the packed PBF format
///
/// <https://wiki.openstreetmap.org/wiki/PBF_Format#Definition_of_OSMData_fileblock>
fn packed_to_degrees(value: i64, granularity: i32, offset: i64) -> Result<Decimal, DecodeError> {
    const SCALE: u32 = 9;

    // SAFETY: these conversions cannot fail
    let mut value = Decimal::from_i64(value).unwrap();
    value.set_scale(SCALE)?;

    let mut granularity = Decimal::from_i32(granularity).unwrap();
    granularity.set_scale(SCALE)?;

    let mut offset = Decimal::from_i64(offset).unwrap();
    offset.set_scale(SCALE)?;

    Ok(offset + (granularity + value))
}

impl From<MemberTypePbf> for MemberType {
    fn from(value: MemberTypePbf) -> Self {
        match value {
            MemberTypePbf::NODE => Self::Node,
            MemberTypePbf::WAY => Self::Way,
            MemberTypePbf::RELATION => Self::Relation,
        }
    }
}

/// Decodes the timestamp and user_sid fields to create the non-pbf representation
fn info_from_pbf(info: &InfoPbf, date_granularity: i32, string_table: &[String]) -> Info {
    Info {
        version: info.version,
        timestamp: info
            .timestamp
            .map(|ts| ts * i64::from(date_granularity))
            .and_then(NaiveDateTime::from_timestamp_millis),
        changeset: info.changeset,
        uid: info.uid,
        user: info.user_sid.and_then(|user_sid| {
            string_table
                .get(user_sid as usize)
                .filter(|s| !s.is_empty())
                .cloned()
        }),
        visible: info.visible,
    }
}
