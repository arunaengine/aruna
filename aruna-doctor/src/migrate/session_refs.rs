//! Decodes stored values whose nested structs predate their later fields, as session references
//! before the display name or the linked login. A missing field decodes as its zero value.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::de::value::{MapDeserializer, SeqDeserializer};
use serde::de::{
    DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use std::fmt;
use std::marker::PhantomData;

/// The serde name of `SessionRef`, whose older layouts lack trailing fields.
const SESSION_REF: &str = "SessionRef";

/// Stored field counts by serde struct name, or by the comma-joined field names of a struct
/// variant, which has no serde name; a listed struct lacks its later fields.
pub(super) type Layout = &'static [(&'static str, usize)];

/// Session references before the linked login, and before the display name too.
pub(super) const NO_LOGIN: Layout = &[(SESSION_REF, 3)];
pub(super) const NO_NAME: Layout = &[(SESSION_REF, 2)];

/// Decodes `bytes` as `T` with every struct `layout` names stored with only its first fields.
/// All bytes must be used.
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], layout: Layout) -> postcard::Result<T> {
    let mut inner = postcard::Deserializer::from_bytes(bytes);
    let value = T::deserialize(Legacy {
        inner: &mut inner,
        layout,
    })?;
    if !inner.finalize()?.is_empty() {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(value)
}

/// Forwards to `inner` and keeps the layout for every nested value.
struct Legacy<D> {
    inner: D,
    layout: Layout,
}

macro_rules! forward {
    ($($method:ident($($arg:ident: $ty:ty),*)),* $(,)?) => {$(
        fn $method<V: Visitor<'de>>(self, $($arg: $ty,)* visitor: V) -> Result<V::Value, D::Error> {
            let layout = self.layout;
            self.inner.$method($($arg,)* Wrap { visitor, layout })
        }
    )*};
}

impl<'de, D: Deserializer<'de>> Deserializer<'de> for Legacy<D> {
    type Error = D::Error;

    forward!(
        deserialize_any(),
        deserialize_bool(),
        deserialize_i8(),
        deserialize_i16(),
        deserialize_i32(),
        deserialize_i64(),
        deserialize_i128(),
        deserialize_u8(),
        deserialize_u16(),
        deserialize_u32(),
        deserialize_u64(),
        deserialize_u128(),
        deserialize_f32(),
        deserialize_f64(),
        deserialize_char(),
        deserialize_str(),
        deserialize_string(),
        deserialize_bytes(),
        deserialize_byte_buf(),
        deserialize_option(),
        deserialize_unit(),
        deserialize_unit_struct(name: &'static str),
        deserialize_newtype_struct(name: &'static str),
        deserialize_seq(),
        deserialize_tuple(len: usize),
        deserialize_tuple_struct(name: &'static str, len: usize),
        deserialize_map(),
        deserialize_enum(name: &'static str, variants: &'static [&'static str]),
        deserialize_identifier(),
        deserialize_ignored_any(),
    );

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        name: &'static str,
        names: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, D::Error> {
        let layout = self.layout;
        let stored = layout.iter().find(|(known, _)| *known == name);
        let Some(&(_, fields)) = stored.filter(|(_, fields)| *fields < names.len()) else {
            return self
                .inner
                .deserialize_struct(name, names, Wrap { visitor, layout });
        };
        let padded = Padded {
            visitor,
            layout,
            fields,
            total: names.len(),
        };
        self.inner.deserialize_tuple(fields, padded)
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

/// Reads the stored fields of a struct and fills the rest with their zero values.
struct Padded<V> {
    visitor: V,
    layout: Layout,
    fields: usize,
    total: usize,
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Padded<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        self.visitor.expecting(formatter)
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        self.visitor.visit_seq(PaddedSeq {
            seq,
            layout: self.layout,
            fields: self.fields,
            total: self.total,
            index: 0,
        })
    }
}

struct PaddedSeq<A> {
    seq: A,
    layout: Layout,
    fields: usize,
    total: usize,
    index: usize,
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for PaddedSeq<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        self.index += 1;
        if self.index <= self.fields {
            let layout = self.layout;
            return self.seq.next_element_seed(Seed { seed, layout });
        }
        if self.index <= self.total {
            return seed.deserialize(Zero(PhantomData)).map(Some);
        }
        Ok(None)
    }
}

/// The value of a field an older layout lacks: none, false, zero or empty.
struct Zero<E>(PhantomData<E>);

impl<'de, E: serde::de::Error> Deserializer<'de> for Zero<E> {
    type Error = E;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_unit()
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_none()
    }

    fn deserialize_bool<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_bool(false)
    }

    fn deserialize_u64<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_u64(0)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_seq(SeqDeserializer::new(std::iter::empty::<()>()))
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, E> {
        visitor.visit_map(MapDeserializer::new(std::iter::empty::<((), ())>()))
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, E> {
        visitor.visit_newtype_struct(self)
    }

    serde::forward_to_deserialize_any! {
        i8 i16 i32 i64 i128 u8 u16 u32 u128 f32 f64 char str string bytes byte_buf unit
        unit_struct tuple tuple_struct struct enum identifier ignored_any
    }
}

/// A visitor whose nested deserializers keep the layout.
struct Wrap<V> {
    visitor: V,
    layout: Layout,
}

macro_rules! visit {
    ($($method:ident($ty:ty)),* $(,)?) => {$(
        fn $method<E: serde::de::Error>(self, value: $ty) -> Result<V::Value, E> {
            self.visitor.$method(value)
        }
    )*};
}

impl<'de, V: Visitor<'de>> Visitor<'de> for Wrap<V> {
    type Value = V::Value;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        self.visitor.expecting(formatter)
    }

    visit!(
        visit_bool(bool),
        visit_i8(i8),
        visit_i16(i16),
        visit_i32(i32),
        visit_i64(i64),
        visit_i128(i128),
        visit_u8(u8),
        visit_u16(u16),
        visit_u32(u32),
        visit_u64(u64),
        visit_u128(u128),
        visit_f32(f32),
        visit_f64(f64),
        visit_char(char),
        visit_str(&str),
        visit_borrowed_str(&'de str),
        visit_string(String),
        visit_bytes(&[u8]),
        visit_borrowed_bytes(&'de [u8]),
        visit_byte_buf(Vec<u8>),
    );

    fn visit_none<E: serde::de::Error>(self) -> Result<V::Value, E> {
        self.visitor.visit_none()
    }

    fn visit_unit<E: serde::de::Error>(self) -> Result<V::Value, E> {
        self.visitor.visit_unit()
    }

    fn visit_some<D: Deserializer<'de>>(self, inner: D) -> Result<V::Value, D::Error> {
        let layout = self.layout;
        self.visitor.visit_some(Legacy { inner, layout })
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, inner: D) -> Result<V::Value, D::Error> {
        let layout = self.layout;
        self.visitor.visit_newtype_struct(Legacy { inner, layout })
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        let layout = self.layout;
        self.visitor.visit_seq(Access { inner: seq, layout })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        let layout = self.layout;
        self.visitor.visit_map(Access { inner: map, layout })
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        let layout = self.layout;
        self.visitor.visit_enum(Access {
            inner: data,
            layout,
        })
    }
}

/// A seed whose deserializer keeps the layout.
struct Seed<S> {
    seed: S,
    layout: Layout,
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, inner: D) -> Result<S::Value, D::Error> {
        let layout = self.layout;
        self.seed.deserialize(Legacy { inner, layout })
    }
}

/// Sequence, map, enum and variant access whose elements keep the layout.
struct Access<A> {
    inner: A,
    layout: Layout,
}

impl<A> Access<A> {
    fn seed<S>(&self, seed: S) -> Seed<S> {
        Seed {
            seed,
            layout: self.layout,
        }
    }
}

impl<'de, A: SeqAccess<'de>> SeqAccess<'de> for Access<A> {
    type Error = A::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, A::Error> {
        let seed = self.seed(seed);
        self.inner.next_element_seed(seed)
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: MapAccess<'de>> MapAccess<'de> for Access<A> {
    type Error = A::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, A::Error> {
        let seed = self.seed(seed);
        self.inner.next_key_seed(seed)
    }

    fn next_value_seed<T: DeserializeSeed<'de>>(&mut self, seed: T) -> Result<T::Value, A::Error> {
        let seed = self.seed(seed);
        self.inner.next_value_seed(seed)
    }

    fn size_hint(&self) -> Option<usize> {
        self.inner.size_hint()
    }
}

impl<'de, A: EnumAccess<'de>> EnumAccess<'de> for Access<A> {
    type Error = A::Error;
    type Variant = Access<A::Variant>;

    fn variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<(T::Value, Self::Variant), A::Error> {
        let layout = self.layout;
        let (value, inner) = self.inner.variant_seed(Seed { seed, layout })?;
        Ok((value, Access { inner, layout }))
    }
}

impl<'de, A: VariantAccess<'de>> VariantAccess<'de> for Access<A> {
    type Error = A::Error;

    fn unit_variant(self) -> Result<(), A::Error> {
        self.inner.unit_variant()
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(self, seed: T) -> Result<T::Value, A::Error> {
        let seed = self.seed(seed);
        self.inner.newtype_variant_seed(seed)
    }

    fn tuple_variant<V: Visitor<'de>>(self, len: usize, visitor: V) -> Result<V::Value, A::Error> {
        let layout = self.layout;
        self.inner.tuple_variant(len, Wrap { visitor, layout })
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        names: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        let layout = self.layout;
        let stored = layout.iter().find(|(known, _)| *known == names.join(","));
        let Some(&(_, fields)) = stored.filter(|(_, fields)| *fields < names.len()) else {
            return self.inner.struct_variant(names, Wrap { visitor, layout });
        };
        let padded = Padded {
            visitor,
            layout,
            fields,
            total: names.len(),
        };
        self.inner.tuple_variant(fields, padded)
    }
}
