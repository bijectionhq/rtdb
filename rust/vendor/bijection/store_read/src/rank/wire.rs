//! Serde helpers that keep floats exact on any JSON codec: a float travels
//! as its IEEE bits.

pub mod f64_bits {
    pub fn serialize<S: serde::Serializer>(value: &f64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(value.to_bits())
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<f64, D::Error> {
        Ok(f64::from_bits(<u64 as serde::Deserialize>::deserialize(deserializer)?))
    }
}

pub mod option_f64_bits {
    pub fn serialize<S: serde::Serializer>(value: &Option<f64>, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&value.map(f64::to_bits), serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<f64>, D::Error> {
        Ok(<Option<u64> as serde::Deserialize>::deserialize(deserializer)?.map(f64::from_bits))
    }
}

pub mod f32_vec_bits {
    pub fn serialize<S: serde::Serializer>(value: &[f32], serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&value.iter().map(|value| value.to_bits()).collect::<Vec<_>>(), serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Vec<f32>, D::Error> {
        Ok(<Vec<u32> as serde::Deserialize>::deserialize(deserializer)?.into_iter().map(f32::from_bits).collect())
    }
}

pub mod option_f32_vec_bits {
    pub fn serialize<S: serde::Serializer>(value: &Option<Vec<f32>>, serializer: S) -> Result<S::Ok, S::Error> {
        serde::Serialize::serialize(&value.as_ref().map(|value| value.iter().map(|value| value.to_bits()).collect::<Vec<_>>()), serializer)
    }

    pub fn deserialize<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Option<Vec<f32>>, D::Error> {
        Ok(<Option<Vec<u32>> as serde::Deserialize>::deserialize(deserializer)?.map(|value| value.into_iter().map(f32::from_bits).collect()))
    }
}
