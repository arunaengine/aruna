//! Decodes stored values whose nested session references predate the display name or the
//! linked login. The missing trailing fields decode as `None`; everything else is unchanged.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use serde::de::value::UnitDeserializer;
use serde::de::{
    DeserializeOwned, DeserializeSeed, Deserializer, EnumAccess, MapAccess, SeqAccess,
    VariantAccess, Visitor,
};
use std::fmt;

/// The serde name of `SessionRef`, whose older layouts lack trailing fields.
const SESSION_REF: &str = "SessionRef";

/// Decodes `bytes` as `T` with every session reference stored with only its first `fields`
/// fields: 2 before the display name, 3 before the linked login. All bytes must be used.
pub(super) fn decode<T: DeserializeOwned>(bytes: &[u8], fields: usize) -> postcard::Result<T> {
    let mut inner = postcard::Deserializer::from_bytes(bytes);
    let value = T::deserialize(Legacy {
        inner: &mut inner,
        fields,
    })?;
    if !inner.finalize()?.is_empty() {
        return Err(postcard::Error::DeserializeBadEncoding);
    }
    Ok(value)
}

/// Forwards to `inner` and keeps the layout for every nested value.
struct Legacy<D> {
    inner: D,
    fields: usize,
}

macro_rules! forward {
    ($($method:ident($($arg:ident: $ty:ty),*)),* $(,)?) => {$(
        fn $method<V: Visitor<'de>>(self, $($arg: $ty,)* visitor: V) -> Result<V::Value, D::Error> {
            let fields = self.fields;
            self.inner.$method($($arg,)* Wrap { visitor, fields })
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
        let fields = self.fields;
        if name != SESSION_REF || fields >= names.len() {
            return self
                .inner
                .deserialize_struct(name, names, Wrap { visitor, fields });
        }
        let padded = Padded {
            visitor,
            fields,
            total: names.len(),
        };
        self.inner.deserialize_tuple(fields, padded)
    }

    fn is_human_readable(&self) -> bool {
        self.inner.is_human_readable()
    }
}

/// Reads the stored fields of a session reference and fills the rest with `None`.
struct Padded<V> {
    visitor: V,
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
            fields: self.fields,
            total: self.total,
            index: 0,
        })
    }
}

struct PaddedSeq<A> {
    seq: A,
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
            let fields = self.fields;
            return self.seq.next_element_seed(Seed { seed, fields });
        }
        if self.index <= self.total {
            return seed.deserialize(UnitDeserializer::new()).map(Some);
        }
        Ok(None)
    }
}

/// A visitor whose nested deserializers keep the layout.
struct Wrap<V> {
    visitor: V,
    fields: usize,
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
        let fields = self.fields;
        self.visitor.visit_some(Legacy { inner, fields })
    }

    fn visit_newtype_struct<D: Deserializer<'de>>(self, inner: D) -> Result<V::Value, D::Error> {
        let fields = self.fields;
        self.visitor.visit_newtype_struct(Legacy { inner, fields })
    }

    fn visit_seq<A: SeqAccess<'de>>(self, seq: A) -> Result<V::Value, A::Error> {
        let fields = self.fields;
        self.visitor.visit_seq(Access { inner: seq, fields })
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<V::Value, A::Error> {
        let fields = self.fields;
        self.visitor.visit_map(Access { inner: map, fields })
    }

    fn visit_enum<A: EnumAccess<'de>>(self, data: A) -> Result<V::Value, A::Error> {
        let fields = self.fields;
        self.visitor.visit_enum(Access {
            inner: data,
            fields,
        })
    }
}

/// A seed whose deserializer keeps the layout.
struct Seed<S> {
    seed: S,
    fields: usize,
}

impl<'de, S: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<S> {
    type Value = S::Value;

    fn deserialize<D: Deserializer<'de>>(self, inner: D) -> Result<S::Value, D::Error> {
        let fields = self.fields;
        self.seed.deserialize(Legacy { inner, fields })
    }
}

/// Sequence, map, enum and variant access whose elements keep the layout.
struct Access<A> {
    inner: A,
    fields: usize,
}

impl<A> Access<A> {
    fn seed<S>(&self, seed: S) -> Seed<S> {
        Seed {
            seed,
            fields: self.fields,
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
        let fields = self.fields;
        let (value, inner) = self.inner.variant_seed(Seed { seed, fields })?;
        Ok((value, Access { inner, fields }))
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
        let fields = self.fields;
        self.inner.tuple_variant(len, Wrap { visitor, fields })
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        names: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, A::Error> {
        let fields = self.fields;
        self.inner.struct_variant(names, Wrap { visitor, fields })
    }
}
