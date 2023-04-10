use std::string::FromUtf8Error;

use crate::protos::osmformat::{
    mod_Relation::MemberType as MemberTypePbf, Info as InfoPbf, PrimitiveBlock, StringTable,
};
use async_stream::try_stream;
use chrono::NaiveDateTime;
use fnv::FnvHashMap as HashMap;
use futures::Stream;
use itertools::izip;
use kstring::KString;
use num_traits::CheckedAdd;
use rust_decimal::Decimal;
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

/// Decode a [PrimitiveBlock] into an [Element] stream
///
/// There is no dependency on order, so this can be called in parallel.
/// Technically, the file format is `(<HeaderBlock> (<PrimitiveBlock>)+)*`, but
/// [FileBlock::Header] and [FileBlock::Other] don't contain any elements.
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
pub fn decode_elements<'a>(
    PrimitiveBlock {
        stringtable: StringTable {
            s: raw_string_table,
        },
        primitivegroup,
        granularity,
        lat_offset,
        lon_offset,
        date_granularity,
    }: PrimitiveBlock,
) -> impl Stream<Item = Result<Element, DecodeError>> + Send + 'a {
    try_stream! {
        let mut string_table = Vec::with_capacity(raw_string_table.len());
        for raw_string in raw_string_table {
            string_table.push(KString::from(String::from_utf8(raw_string)?));
        }

        let date_granularity = i64::from(date_granularity);

        for group in primitivegroup {
            for node in group.nodes {
                yield Element::Node(Node {
                    id: Id(node.id),
                    attributes: kv_to_attributes(node.keys, node.vals, &string_table),
                    info: node
                        .info
                        .map(|info| info_from_pbf(info, date_granularity, &string_table)),
                    lat: packed_to_degrees(node.lat, granularity, lat_offset)?,
                    lon: packed_to_degrees(node.lon, granularity, lon_offset)?,
                });
            }
            if let Some(dense) = group.dense {
                let (mut dense_node_it, mut dense_attrs_it) = {
                    let dense_attrs_size_hint = dense.id.len();

                    let id_it = Delta::from(dense.id.into_iter());
                    let lat_it = Delta::from(dense.lat.into_iter());
                    let lon_it = Delta::from(dense.lon.into_iter());

                    let dense_attrs =
                        dense_kv_to_attributes(dense.keys_vals, &string_table, dense_attrs_size_hint);
                    #[cfg(debug)]
                    {
                        assert_eq!(dense.id.len(), dense.lat.len());
                        assert_eq!(dense.lat.len(), dense.lon.len());
                        if !dense.keys_vals.is_empty() {
                            assert_eq!(
                                dense.lon.len(),
                                dense.keys_vals.iter().filter(|k_or_v| *k_or_v == 0).count()
                            );
                        }
                    }
                    (izip!(id_it, lat_it, lon_it), dense_attrs)
                };

                let mut dense_info_it = if let Some(dense_info) = dense.denseinfo {
                    let version_it = dense_info.version.into_iter();
                    let timestamp_it = Delta::from(dense_info.timestamp.into_iter());
                    let changeset_it = Delta::from(dense_info.changeset.into_iter());
                    let uid_it = Delta::from(dense_info.uid.into_iter());
                    let user_sid_it = Delta::from(dense_info.user_sid.into_iter());
                    let visible_it = dense_info.visible.into_iter();
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
                    Some((
                        izip!(version_it, timestamp_it, changeset_it, uid_it, user_sid_it),
                        visible_it,
                    ))
                } else {
                    None
                };

                while let (Some((id, lat, lon)), attributes) =
                    (dense_node_it.next(), dense_attrs_it.next())
                {
                    yield Element::Node(Node {
                        id: Id(id?),
                        attributes: attributes.unwrap_or_default(),
                        info: if let Some((
                            (version, timestamp, changeset, uid, user_sid),
                            visible,
                        )) = dense_info_it
                            .as_mut()
                            .and_then(|(it, visible_it)| it.next().zip(Some(visible_it.next())))
                        {
                            Some(info_from_pbf(
                                InfoPbf {
                                    version,
                                    timestamp: Some(timestamp?),
                                    changeset: Some(changeset?),
                                    uid: Some(uid?),
                                    // OSM authors indicated that this is incorrectly specified in the proto file
                                    // so it is ok to convert like this.
                                    user_sid: Some(user_sid? as u32),
                                    visible,
                                },
                                date_granularity,
                                &string_table,
                            ))
                        } else {
                            None
                        },
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
                    attributes: kv_to_attributes(way.keys, way.vals, &string_table),
                    info: way
                        .info
                        .map(|info| info_from_pbf(info, date_granularity, &string_table)),
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

                let members = izip!(
                    relation.roles_sid.into_iter(),
                    relation.memids.into_iter(),
                    relation.types.into_iter(),
                ).map(|(role_sid, member_id, ty)| Member {
                    id: Id(member_id),
                    ty: ty.into(),
                    role: string_table
                        .get(role_sid as usize)
                        .filter(|s| !s.is_empty())
                        .cloned(),
                }).collect();

                yield Element::Relation(Relation {
                    id: Id(relation.id),
                    attributes: kv_to_attributes(relation.keys, relation.vals, &string_table),
                    info: relation
                        .info
                        .map(|info| info_from_pbf(info, date_granularity, &string_table)),
                    members,
                })
            }
        }
    }
}

/// Converts key & value index arrays into an attribute map
#[inline]
fn kv_to_attributes(
    keys: Vec<u32>,
    vals: Vec<u32>,
    string_table: &[KString],
) -> HashMap<KString, KString> {
    #[cfg(debug)]
    assert_eq!(keys.len(), vals.len());

    keys.into_iter()
        .zip(vals.into_iter())
        .filter_map(|(k, v)| {
            string_table
                .get(k as usize)
                .cloned()
                .zip(string_table.get(v as usize).cloned())
        })
        .collect()
}

/// Converts a dense key-value array into attribute maps
///
/// This differs from [kv_to_attributes] because the keys and values
/// for _multiple_ nodes are packed into a single array and end-delimited by a `0`.
///
/// <https://wiki.openstreetmap.org/wiki/PBF_Format#Nodes>
#[inline]
fn dense_kv_to_attributes(
    keys_vals: Vec<i32>,
    string_table: &'_ [KString],
    size_hint: usize,
) -> impl Iterator<Item = HashMap<KString, KString>> + '_ {
    struct DenseKVIterator<'a, KI> {
        keys_vals: KI,
        string_table: &'a [KString],
        size_hint: usize,
        key: Option<usize>,
        current: HashMap<KString, KString>,
    }

    impl<'a, KI> Iterator for DenseKVIterator<'a, KI>
    where
        KI: Iterator<Item = i32>,
    {
        type Item = HashMap<KString, KString>;

        #[inline]
        fn next(&mut self) -> Option<Self::Item> {
            for k_or_v in self.keys_vals.by_ref() {
                let k_or_v = k_or_v as usize;
                if k_or_v == 0 {
                    #[cfg(debug)]
                    assert_eq!(self.key, None);
                    let mut ret = HashMap::default();
                    std::mem::swap(&mut self.current, &mut ret);
                    return Some(ret);
                } else if let Some(key) = self.key.take() {
                    if let Some((key, value)) = self
                        .string_table
                        .get(key)
                        .cloned()
                        .zip(self.string_table.get(k_or_v).cloned())
                    {
                        self.current.insert(key, value);
                    }
                } else {
                    self.key = Some(k_or_v);
                }
            }

            #[cfg(debug)]
            {
                assert_eq!(self.key, None);
                assert!(self.current.is_empty());
            }
            None
        }

        #[inline]
        fn size_hint(&self) -> (usize, Option<usize>) {
            (self.size_hint, Some(self.size_hint))
        }
    }

    DenseKVIterator {
        size_hint: {
            if keys_vals.is_empty() {
                0
            } else {
                size_hint
            }
        },
        keys_vals: keys_vals.into_iter(),
        string_table,
        key: None,
        current: HashMap::default(),
    }
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

    #[inline]
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

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iterator.size_hint()
    }
}

/// Latitude/longitude packed storage scale
const SCALE: u32 = 9;

/// Calculates the latitude/longitude degrees from the packed PBF format
///
/// <https://wiki.openstreetmap.org/wiki/PBF_Format#Definition_of_OSMData_fileblock>
#[inline]
fn packed_to_degrees(
    value: i64,
    granularity: i32,
    offset: i64,
) -> Result<Decimal, rust_decimal::Error> {
    let value: i128 = value.into();
    let granularity: i128 = granularity.into();
    let offset: i128 = offset.into();

    let scale_free = value
        .checked_mul(granularity)
        .and_then(|product| product.checked_add(offset))
        .ok_or(rust_decimal::Error::ExceedsMaximumPossibleValue)?;

    Decimal::try_from_i128_with_scale(scale_free, SCALE)
}

impl From<MemberTypePbf> for MemberType {
    #[inline]
    fn from(value: MemberTypePbf) -> Self {
        match value {
            MemberTypePbf::NODE => Self::Node,
            MemberTypePbf::WAY => Self::Way,
            MemberTypePbf::RELATION => Self::Relation,
        }
    }
}

/// Decodes the timestamp and user_sid fields to create the non-pbf representation
#[inline]
fn info_from_pbf(info: InfoPbf, date_granularity: i64, string_table: &[KString]) -> Info {
    Info {
        version: info.version,
        timestamp: info
            .timestamp
            .map(|ts| ts * date_granularity)
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
