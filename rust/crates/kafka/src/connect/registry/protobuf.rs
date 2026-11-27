use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    path::Path,
    rc::Rc,
    sync::Arc,
};

use base64::{
    Engine,
    engine::general_purpose::{STANDARD, STANDARD_NO_PAD},
};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, FileDescriptor, MessageDescriptor};
use protox::{
    Compiler,
    file::{File, FileResolver},
};
use rtdb_columnar::{MemoryBudget, MemoryReservation};
use rtdb_types::Result;

use super::{ConnectData, ConversionContext, error};

mod projection;

pub(super) const MAX_DEPTH: usize = 100;
const ROOT_FILE: &str = "__rtdb_connect_root.proto";

pub(super) struct Frame<'a> {
    indexes: [usize; MAX_DEPTH],
    len: usize,
    pub(super) payload: &'a [u8],
}

impl<'a> Frame<'a> {
    pub(super) fn read(mut bytes: &'a [u8]) -> Result<Self> {
        // Confluent's message-index path uses signed Kafka varints and a shorthand for [0].
        // https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format
        let count = nonnegative_varint(&mut bytes)?;
        if count > MAX_DEPTH || count > bytes.len() {
            return Err(error(
                "conversion-error",
                "Invalid Protobuf message index count",
            ));
        }
        let mut indexes = [0; MAX_DEPTH];
        for index in &mut indexes[..count] {
            *index = nonnegative_varint(&mut bytes)?;
        }
        Ok(Self {
            indexes,
            len: count.max(1),
            payload: bytes,
        })
    }

    pub(super) fn indexes(&self) -> &[usize] {
        &self.indexes[..self.len]
    }
}

fn nonnegative_varint(bytes: &mut &[u8]) -> Result<usize> {
    let invalid = || {
        error(
            "conversion-error",
            "Malformed Protobuf message index varint",
        )
    };
    let mut zigzag = 0_u32;
    for shift in (0..32).step_by(7) {
        let (&byte, remaining) = bytes.split_first().ok_or_else(invalid)?;
        *bytes = remaining;
        if shift == 28 && byte > 0x0f {
            return Err(invalid());
        }
        zigzag |= u32::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            if zigzag & 1 != 0 {
                return Err(error("conversion-error", "Negative Protobuf message index"));
            }
            return Ok((zigzag >> 1) as usize);
        }
    }
    Err(invalid())
}

pub(super) struct Document {
    root: FileDescriptor,
    charge: Arc<MemoryReservation>,
}

impl Document {
    pub(super) fn parse(
        source: &str,
        references: &BTreeMap<String, String>,
        context: &mut ConversionContext,
    ) -> Result<Self> {
        let input_bytes = references
            .iter()
            .try_fold(source.len(), |total, (name, text)| {
                total.checked_add(name.len())?.checked_add(text.len())
            })
            .ok_or_else(overflow)?;
        let budget = context.scratch_budget();
        let scratch = budget.reserve(parser_bound(input_bytes)?)?;
        let mut root_name = ROOT_FILE.to_owned();
        while references.contains_key(&root_name) {
            root_name.insert(0, '_');
        }
        let root = parse_file(&root_name, source)?;
        let root_name = root.name().to_owned();
        let mut files = BTreeMap::from([(root_name.clone(), root)]);
        for (name, text) in references {
            let file = parse_file(name, text)?;
            if file.name() != name || files.insert(name.clone(), file).is_some() {
                return Err(error(
                    "schema-registry",
                    format!("Conflicting Protobuf import name '{name}'"),
                ));
            }
        }
        check_import_depth(
            &root_name,
            &files,
            &mut BTreeSet::new(),
            &mut BTreeMap::new(),
        )?;
        let admission = Rc::new(ImportAdmission {
            budget,
            charges: RefCell::new(Vec::new()),
            failure: RefCell::new(None),
        });
        let resolver = Imports {
            files,
            admission: Rc::clone(&admission),
        };
        let mut compiler = Compiler::with_file_resolver(resolver);
        compiler.open_file(&root_name).map_err(|cause| {
            admission
                .failure
                .borrow_mut()
                .take()
                .unwrap_or_else(|| error("schema-registry", cause))
        })?;
        let pool = compiler.descriptor_pool();
        let root = pool.get_file_by_name(&root_name).ok_or_else(|| {
            error(
                "schema-registry",
                "Compiled Protobuf schema has no root file",
            )
        })?;
        let descriptor_bytes = pool
            .files()
            .try_fold(4096usize, |total, file| {
                total.checked_add(file.file_descriptor_proto().encoded_len().checked_mul(64)?)
            })
            .ok_or_else(overflow)?;
        drop(compiler);
        let charge = context.reserve(descriptor_bytes)?;
        drop(admission);
        drop(scratch);
        Ok(Self { root, charge })
    }

    pub(super) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        context.retain(Arc::clone(&self.charge))
    }

    pub(super) fn message(&self, indexes: &[usize]) -> Result<MessageDescriptor> {
        let invalid = || {
            error(
                "conversion-error",
                "Protobuf message indexes do not select a declared message",
            )
        };
        let (&first, nested) = indexes.split_first().ok_or_else(invalid)?;
        let mut message = self.root.messages().nth(first).ok_or_else(invalid)?;
        for &index in nested {
            let child = message
                .child_messages()
                .filter(|child| {
                    // A map field introduces a synthetic descriptor but no entry in Confluent's index path.
                    !child.is_map_entry()
                        || self
                            .root
                            .file_descriptor_proto()
                            .source_code_info
                            .as_ref()
                            .is_some_and(|source| {
                                source
                                    .location
                                    .iter()
                                    .any(|location| location.path.as_slice() == child.path())
                            })
                })
                .nth(index)
                .ok_or_else(invalid)?;
            message = child;
        }
        Ok(message)
    }
}

fn parse_file(name: &str, source: &str) -> Result<File> {
    check_source_depth(source)?;
    match File::from_source(name, source) {
        Ok(file) => Ok(file),
        Err(cause) => {
            let descriptor = STANDARD
                .decode(source)
                .or_else(|_| STANDARD_NO_PAD.decode(source))
                .ok()
                .and_then(|bytes| File::decode_file_descriptor_proto(bytes.as_slice()).ok())
                .filter(|file| !file.name().is_empty());
            descriptor.ok_or_else(|| error("schema-registry", cause))
        }
    }
}

fn check_import_depth<'a>(
    name: &'a str,
    files: &'a BTreeMap<String, File>,
    active: &mut BTreeSet<&'a str>,
    heights: &mut BTreeMap<&'a str, usize>,
) -> Result<usize> {
    if let Some(&height) = heights.get(name) {
        return Ok(height);
    }
    let Some(file) = files.get(name) else {
        return Ok(0);
    };
    if active.len() >= MAX_DEPTH || !active.insert(name) {
        return Err(error(
            "schema-registry",
            "Protobuf imports exceed their nesting limit or form a cycle",
        ));
    }
    let mut height = 1usize;
    for dependency in &file.file_descriptor_proto().dependency {
        height = height.max(1 + check_import_depth(dependency, files, active, heights)?);
    }
    if height > MAX_DEPTH {
        return Err(error(
            "schema-registry",
            "Protobuf import nesting limit exceeded",
        ));
    }
    active.remove(name);
    heights.insert(name, height);
    Ok(height)
}

fn check_source_depth(source: &str) -> Result<()> {
    let mut bytes = source.bytes().peekable();
    let mut depth = 0usize;
    while let Some(byte) = bytes.next() {
        match byte {
            b'/' if bytes.next_if_eq(&b'/').is_some() => {
                for byte in bytes.by_ref() {
                    if byte == b'\n' {
                        break;
                    }
                }
            }
            b'/' if bytes.next_if_eq(&b'*').is_some() => {
                while let Some(byte) = bytes.next() {
                    if byte == b'*' && bytes.next_if_eq(&b'/').is_some() {
                        break;
                    }
                }
            }
            quote @ (b'"' | b'\'') => {
                while let Some(byte) = bytes.next() {
                    if byte == quote {
                        break;
                    }
                    if byte == b'\\' {
                        bytes.next();
                    }
                }
            }
            b'{' | b'[' | b'<' | b'(' => {
                depth += 1;
                if depth > MAX_DEPTH {
                    return Err(error(
                        "schema-registry",
                        "Protobuf schema nesting limit exceeded",
                    ));
                }
            }
            b'}' | b']' | b'>' | b')' => depth = depth.saturating_sub(1),
            _ => (),
        }
    }
    Ok(())
}

struct Imports {
    files: BTreeMap<String, File>,
    admission: Rc<ImportAdmission>,
}

struct ImportAdmission {
    budget: Arc<MemoryBudget>,
    charges: RefCell<Vec<Arc<MemoryReservation>>>,
    failure: RefCell<Option<rtdb_types::Anomaly>>,
}

impl ImportAdmission {
    fn reserve(&self, name: &str, bytes: usize) -> std::result::Result<(), protox::Error> {
        match parser_bound(bytes).and_then(|bytes| self.budget.reserve(bytes)) {
            Ok(charge) => {
                self.charges.borrow_mut().push(charge);
                Ok(())
            }
            Err(cause) => {
                *self.failure.borrow_mut() = Some(cause);
                Err(protox::Error::file_not_found(name))
            }
        }
    }
}

impl FileResolver for Imports {
    fn resolve_path(&self, path: &Path) -> Option<String> {
        path.to_str().map(str::to_owned)
    }

    fn open_file(&self, name: &str) -> std::result::Result<File, protox::Error> {
        if let Some(file) = self.files.get(name) {
            return Ok(file.clone());
        }
        if let Some(source) = known_proto(name) {
            self.admission.reserve(name, source.len())?;
            return File::from_source(name, source);
        }
        let file = DescriptorPool::global()
            .get_file_by_name(name)
            .ok_or_else(|| protox::Error::file_not_found(name))?;
        self.admission
            .reserve(name, file.file_descriptor_proto().encoded_len())?;
        Ok(File::from_file_descriptor_proto(
            file.file_descriptor_proto().clone(),
        ))
    }
}

fn known_proto(name: &str) -> Option<&'static str> {
    Some(match name {
        "confluent/meta.proto" => {
            r#"
            syntax = "proto3";
            package confluent;
            import "google/protobuf/descriptor.proto";
            message Meta {
                string doc = 1;
                map<string, string> params = 2;
                repeated string tags = 3;
            }
            extend google.protobuf.FileOptions { Meta file_meta = 1088; }
            extend google.protobuf.MessageOptions { Meta message_meta = 1088; }
            extend google.protobuf.FieldOptions { Meta field_meta = 1088; }
            extend google.protobuf.EnumOptions { Meta enum_meta = 1088; }
            extend google.protobuf.EnumValueOptions { Meta enum_value_meta = 1088; }
        "#
        }
        "confluent/type/decimal.proto" => {
            r#"
            syntax = "proto3";
            package confluent.type;
            message Decimal {
                bytes value = 1;
                uint32 precision = 2;
                int32 scale = 3;
            }
        "#
        }
        "google/type/calendar_period.proto" => {
            include_str!("protobuf/google/calendar_period.proto")
        }
        "google/type/color.proto" => include_str!("protobuf/google/color.proto"),
        "google/type/date.proto" => include_str!("protobuf/google/date.proto"),
        "google/type/datetime.proto" => include_str!("protobuf/google/datetime.proto"),
        "google/type/dayofweek.proto" => include_str!("protobuf/google/dayofweek.proto"),
        "google/type/decimal.proto" => include_str!("protobuf/google/decimal.proto"),
        "google/type/expr.proto" => include_str!("protobuf/google/expr.proto"),
        "google/type/fraction.proto" => include_str!("protobuf/google/fraction.proto"),
        "google/type/interval.proto" => include_str!("protobuf/google/interval.proto"),
        "google/type/latlng.proto" => include_str!("protobuf/google/latlng.proto"),
        "google/type/money.proto" => include_str!("protobuf/google/money.proto"),
        "google/type/month.proto" => include_str!("protobuf/google/month.proto"),
        "google/type/phone_number.proto" => include_str!("protobuf/google/phone_number.proto"),
        "google/type/postal_address.proto" => include_str!("protobuf/google/postal_address.proto"),
        "google/type/quaternion.proto" => include_str!("protobuf/google/quaternion.proto"),
        "google/type/timeofday.proto" => include_str!("protobuf/google/timeofday.proto"),
        _ => return None,
    })
}

pub(super) use projection::{Options, Projection};

pub(super) fn decode_message(
    descriptor: MessageDescriptor,
    bytes: &[u8],
    context: &mut ConversionContext,
) -> Result<DynamicMessage> {
    context.reserve(
        bytes
            .len()
            .checked_mul(256)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(overflow)?,
    )?;
    let message = DynamicMessage::decode(descriptor, bytes)
        .map_err(|cause| error("conversion-error", cause))?;
    projection::validate_required(&message)?;
    Ok(message)
}

pub(super) fn migrate(
    message: &DynamicMessage,
    reader: MessageDescriptor,
    encoded_bytes: usize,
    context: &mut ConversionContext,
) -> Result<DynamicMessage> {
    context.reserve(
        encoded_bytes
            .checked_mul(512)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(overflow)?,
    )?;
    let json = serde_json::to_value(message).map_err(|cause| error("conversion-error", cause))?;
    let result = DynamicMessage::deserialize(reader, json)
        .map_err(|cause| error("conversion-error", cause))?;
    projection::validate_required(&result)?;
    Ok(result)
}

pub(super) fn message_json(
    message: &DynamicMessage,
    encoded_bytes: usize,
    context: &mut ConversionContext,
) -> Result<super::super::json_value::JsonValue> {
    context.reserve(
        encoded_bytes
            .checked_mul(512)
            .and_then(|bytes| bytes.checked_add(4096))
            .ok_or_else(overflow)?,
    )?;
    serde_json::to_value(message)
        .map(super::super::json_value::JsonValue::from_serde)
        .map_err(|cause| error("conversion-error", cause))
}

pub(super) fn overflow() -> rtdb_types::Anomaly {
    error(
        "allocation-overflow",
        "Protobuf converter allocation overflow",
    )
}

fn parser_bound(bytes: usize) -> Result<usize> {
    bytes
        .checked_mul(256)
        .and_then(|bytes| bytes.checked_add(65_536))
        .ok_or_else(overflow)
}
