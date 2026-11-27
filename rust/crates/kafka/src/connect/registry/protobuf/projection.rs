use std::{collections::BTreeMap, sync::Arc};

use prost_reflect::{
    Cardinality, DynamicMessage, FieldDescriptor, Kind, MapKey, MessageDescriptor, OneofDescriptor,
    ReflectMessage, Syntax, Value as ProtoValue,
};
use rtdb_columnar::MemoryReservation;
use rtdb_types::{DecimalValue, ListBuilder, Result, StructFields, Value};

use super::{ConnectData, ConversionContext, MAX_DEPTH, error, overflow};
use crate::connect::{
    ConnectSchema, SchemaKind,
    converter::option_bool,
    data::date_value,
    schema_graph::{GraphBuilder, Metadata, Node, NodeId, NodeKind, Scalar},
};

const TAG: &str = "io.confluent.connect.protobuf.Tag";
const TYPE: &str = "io.confluent.connect.protobuf.Type";
const ENUM: &str = "io.confluent.connect.protobuf.Enum";
const UNION: &str = "io.confluent.connect.protobuf.Union";
const DATE: &str = "org.apache.kafka.connect.data.Date";
const TIME: &str = "org.apache.kafka.connect.data.Time";
const TIMESTAMP: &str = "org.apache.kafka.connect.data.Timestamp";
const DECIMAL: &str = "org.apache.kafka.connect.data.Decimal";

pub(in crate::connect::registry) struct Options {
    pub(in crate::connect::registry) schema_cache_capacity: usize,
    enhanced_names: bool,
    integer_enums: bool,
    optional_label_for_nulls: bool,
    proto2_optional: bool,
    nullable_wrappers: bool,
    primitive_wrappers: bool,
    generate_absent_messages: bool,
    index_unions: bool,
    flatten_unions: bool,
    generalized_sum_types: bool,
}

impl Options {
    pub(in crate::connect::registry) fn parse(
        options: &BTreeMap<String, String>,
        is_key: bool,
    ) -> Result<Self> {
        let get = |key, default| option_bool(options, key, default);
        let class_option = if is_key {
            "specific.protobuf.key.type"
        } else {
            "specific.protobuf.value.type"
        };
        if get("derive.type", false)?
            || options
                .get(class_option)
                .is_some_and(|class| !class.trim().is_empty())
        {
            return Err(error(
                "invalid-plugin-config",
                "Native Protobuf conversion requires dynamic message types",
            ));
        }
        get("scrub.invalid.names", false)?;
        get("ignore.default.for.nullables", false)?;
        let schema_cache_capacity = options
            .get("schemas.cache.config")
            .map(|value| {
                value
                    .trim()
                    .parse::<i32>()
                    .map(|value| value.max(1) as usize)
                    .map_err(|cause| error("invalid-plugin-config", cause))
            })
            .transpose()?
            .unwrap_or(1000);
        Ok(Self {
            schema_cache_capacity,
            enhanced_names: get("enhanced.protobuf.schema.support", false)?,
            integer_enums: get("int.for.enums", false)?,
            optional_label_for_nulls: get("optional.for.nullables", false)?,
            proto2_optional: get("optional.for.proto2", true)?,
            nullable_wrappers: get("wrapper.for.nullables", false)?,
            primitive_wrappers: get("wrapper.for.raw.primitives", true)?,
            generate_absent_messages: get("generate.struct.for.nulls", false)?,
            index_unions: get("generate.index.for.unions", true)?,
            flatten_unions: get("flatten.unions", false)?,
            generalized_sum_types: get("generalized.sum.type.support", false)?,
        })
    }

    fn union_name(&self, union: &OneofDescriptor) -> String {
        if self.index_unions {
            format!(
                "{}_{}",
                union.name(),
                union.path().last().expect("oneof index")
            )
        } else {
            union.name().to_owned()
        }
    }
}

pub(in crate::connect::registry) struct Projection {
    schema: Arc<ConnectSchema>,
    charge: Arc<MemoryReservation>,
}

impl Projection {
    pub(in crate::connect::registry) fn compile(
        descriptor: &MessageDescriptor,
        version: i32,
        options: &Options,
        context: &mut ConversionContext,
    ) -> Result<Self> {
        let mut builder = SchemaBuilder {
            graph: GraphBuilder::new(),
            messages: BTreeMap::new(),
            options,
            context,
        };
        let kind = if let Some(wrapper) = wrapper(descriptor).filter(|_| options.primitive_wrappers)
        {
            NodeKind::Scalar(wrapper)
        } else {
            NodeKind::Alias(builder.message(descriptor, 0)?)
        };
        let root = builder.graph.insert(Node {
            kind,
            metadata: Metadata {
                version: Some(version),
                ..Metadata::default()
            },
        });
        let schema = builder
            .graph
            .finish_with(root, |_, schema| Ok(Metadata::from_schema(schema)))?;
        let charge = context.reserve(
            schema
                .retained_bytes()?
                .checked_add(descriptor.full_name().len())
                .and_then(|bytes| bytes.checked_add(1024))
                .ok_or_else(overflow)?,
        )?;
        Ok(Self { schema, charge })
    }

    pub(in crate::connect::registry) fn convert(
        &self,
        message: DynamicMessage,
        options: &Options,
        context: &mut ConversionContext,
    ) -> Result<ConnectData> {
        context.retain(Arc::clone(&self.charge))?;
        let descriptor = message.descriptor();
        let value = convert(
            &ProtoValue::Message(message),
            &Kind::Message(descriptor),
            &self.schema,
            options,
            context,
            0,
        )?;
        Ok(ConnectData {
            schema: Some(Arc::clone(&self.schema)),
            value,
        })
    }
}

struct SchemaBuilder<'a> {
    graph: GraphBuilder,
    messages: BTreeMap<String, NodeId>,
    options: &'a Options,
    context: &'a mut ConversionContext,
}

impl SchemaBuilder<'_> {
    fn message(&mut self, descriptor: &MessageDescriptor, depth: usize) -> Result<NodeId> {
        if let Some(&id) = self.messages.get(descriptor.full_name()) {
            return Ok(id);
        }
        check_depth(depth)?;
        self.context.reserve(
            descriptor
                .full_name()
                .len()
                .checked_mul(8)
                .and_then(|bytes| bytes.checked_add(8192))
                .ok_or_else(overflow)?,
        )?;
        let id = self.graph.reserve();
        self.messages.insert(descriptor.full_name().to_owned(), id);
        let mut fields = Vec::new();
        for union in descriptor.oneofs().filter(|union| !union.is_synthetic()) {
            let name = self.options.union_name(&union);
            self.context.reserve(
                union
                    .full_name()
                    .len()
                    .checked_mul(8)
                    .and_then(|bytes| bytes.checked_add(8192))
                    .ok_or_else(overflow)?,
            )?;
            let members = declared_fields(descriptor)
                .filter(|field| field.containing_oneof().as_ref() == Some(&union))
                .map(|field| Ok((field.name().into(), self.field(&field, depth + 1)?)))
                .collect::<Result<Vec<_>>>()?;
            if self.options.flatten_unions {
                fields.extend(members);
            } else {
                let mut metadata = Metadata {
                    optional: true,
                    ..Metadata::default()
                };
                metadata.name = Some(if self.options.generalized_sum_types {
                    metadata.parameters.insert(
                        "org.apache.kafka.connect.data.Union".into(),
                        union.name().into(),
                    );
                    union.name().to_owned()
                } else {
                    format!("{UNION}.{}", self.name(union.name(), union.full_name()))
                });
                let member = self.graph.insert(Node {
                    kind: NodeKind::Struct(members),
                    metadata,
                });
                fields.push((name, member));
            }
        }
        for field in declared_fields(descriptor).filter(|field| real_oneof(field).is_none()) {
            fields.push((field.name().to_owned(), self.field(&field, depth + 1)?));
        }
        self.graph.define(
            id,
            Node {
                kind: NodeKind::Struct(fields),
                metadata: Metadata {
                    name: Some(
                        self.name(descriptor.name(), descriptor.full_name())
                            .to_owned(),
                    ),
                    ..Metadata::default()
                },
            },
        )?;
        Ok(id)
    }

    fn name<'a>(&self, short: &'a str, full: &'a str) -> &'a str {
        if self.options.enhanced_names {
            full
        } else {
            short
        }
    }

    fn field(&mut self, field: &FieldDescriptor, depth: usize) -> Result<NodeId> {
        check_depth(depth)?;
        self.context.reserve(
            field
                .full_name()
                .len()
                .checked_mul(8)
                .and_then(|bytes| bytes.checked_add(8192))
                .ok_or_else(overflow)?,
        )?;
        if field.is_group() {
            return Err(error(
                "conversion-error",
                "Protobuf groups have no Connect representation",
            ));
        }
        let options = self.options;
        let mut metadata = Metadata::default();
        let kind = match field.kind() {
            Kind::Message(message) => {
                metadata.optional = true;
                if let Some((key, value)) = map_fields(field, &message) {
                    let name = self.name(message.name(), message.full_name());
                    metadata.name = Some(map_name(name));
                    NodeKind::Map(self.field(&key, depth + 1)?, self.field(&value, depth + 1)?)
                } else if let Some((name, kind)) = logical(&message) {
                    metadata.name = Some(name.into());
                    if name == DECIMAL {
                        let params = field_meta(field);
                        let scale = meta_param(&params, "scale")
                            .and_then(|value| value.parse::<i32>().ok())
                            .unwrap_or(0);
                        metadata
                            .parameters
                            .insert("scale".into(), scale.to_string());
                        if let Some(precision) = meta_param(&params, "precision")
                            .and_then(|value| value.parse::<i32>().ok())
                        {
                            metadata
                                .parameters
                                .insert("connect.decimal.precision".into(), precision.to_string());
                        }
                    }
                    NodeKind::Scalar(kind)
                } else if let Some(wrapper) = wrapper(&message)
                    .filter(|_| options.nullable_wrappers || options.primitive_wrappers)
                {
                    NodeKind::Scalar(wrapper)
                } else {
                    NodeKind::Alias(self.message(&message, depth + 1)?)
                }
            }
            Kind::Enum(enumeration) => {
                metadata.optional = true;
                metadata.name = Some(
                    self.name(enumeration.name(), enumeration.full_name())
                        .to_owned(),
                );
                let key = if options.generalized_sum_types {
                    "org.apache.kafka.connect.data.Enum"
                } else {
                    ENUM
                };
                metadata
                    .parameters
                    .insert(key.into(), enumeration.name().into());
                let bytes = enumeration
                    .values()
                    .try_fold(0usize, |bytes, member| {
                        bytes.checked_add(member.name().len().checked_mul(4)?.checked_add(2048)?)
                    })
                    .ok_or_else(overflow)?;
                self.context.reserve(bytes)?;
                for member in enumeration.values() {
                    metadata.parameters.insert(
                        format!("{key}.{}", member.name()),
                        member.number().to_string(),
                    );
                }
                NodeKind::Scalar(if options.integer_enums {
                    Scalar::I32
                } else {
                    Scalar::String
                })
            }
            kind => {
                let mut scalar = scalar(&kind)?;
                if matches!(kind, Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) {
                    scalar = match meta_param(&field_meta(field), "connect.type") {
                        Some("int8") => Scalar::I8,
                        Some("int16") => Scalar::I16,
                        _ => scalar,
                    };
                }
                if !matches!(scalar, Scalar::I8 | Scalar::I16) {
                    if let Some(name) = wire_type(&kind) {
                        metadata.parameters.insert(TYPE.into(), name.into());
                    }
                }
                NodeKind::Scalar(scalar)
            }
        };
        let kind = if field.is_list() && !matches!(kind, NodeKind::Map(..)) {
            metadata.optional = true;
            let element = self.graph.insert(Node { kind, metadata });
            metadata = Metadata {
                optional: true,
                ..Metadata::default()
            };
            NodeKind::Array(element)
        } else {
            kind
        };
        metadata.optional |= if options.optional_label_for_nulls {
            has_optional_keyword(field)
        } else {
            !options.nullable_wrappers
        };
        metadata
            .parameters
            .insert(TAG.into(), field.number().to_string());
        Ok(self.graph.insert(Node { kind, metadata }))
    }
}

fn declared_fields(descriptor: &MessageDescriptor) -> impl Iterator<Item = FieldDescriptor> + '_ {
    descriptor.descriptor_proto().field.iter().map(|field| {
        descriptor
            .get_field(field.number() as u32)
            .expect("validated field descriptor")
    })
}

fn real_oneof(field: &FieldDescriptor) -> Option<OneofDescriptor> {
    field
        .containing_oneof()
        .filter(|union| !union.is_synthetic())
}

fn has_optional_keyword(field: &FieldDescriptor) -> bool {
    field.field_descriptor_proto().proto3_optional()
        || (field.parent_file().syntax() == Syntax::Proto2
            && field.cardinality() == Cardinality::Optional
            && real_oneof(field).is_none())
}

fn scalar(kind: &Kind) -> Result<Scalar> {
    Ok(match kind {
        Kind::Bool => Scalar::Bool,
        Kind::Int32 | Kind::Sint32 | Kind::Sfixed32 => Scalar::I32,
        Kind::Int64
        | Kind::Sint64
        | Kind::Sfixed64
        | Kind::Uint32
        | Kind::Fixed32
        | Kind::Uint64
        | Kind::Fixed64 => Scalar::I64,
        Kind::Float => Scalar::F32,
        Kind::Double => Scalar::F64,
        Kind::String => Scalar::String,
        Kind::Bytes => Scalar::Bytes,
        _ => return Err(error("conversion-error", "Invalid Protobuf scalar type")),
    })
}

fn wire_type(kind: &Kind) -> Option<&'static str> {
    Some(match kind {
        Kind::Sint32 => "sint32",
        Kind::Sfixed32 => "sfixed32",
        Kind::Uint32 => "uint32",
        Kind::Fixed32 => "fixed32",
        Kind::Uint64 => "uint64",
        Kind::Sint64 => "sint64",
        Kind::Fixed64 => "fixed64",
        Kind::Sfixed64 => "sfixed64",
        _ => return None,
    })
}

fn wrapper(message: &MessageDescriptor) -> Option<Scalar> {
    Some(match message.full_name() {
        "google.protobuf.DoubleValue" => Scalar::F64,
        "google.protobuf.FloatValue" => Scalar::F32,
        "google.protobuf.Int64Value"
        | "google.protobuf.UInt64Value"
        | "google.protobuf.UInt32Value" => Scalar::I64,
        "google.protobuf.Int32Value" => Scalar::I32,
        "google.protobuf.BoolValue" => Scalar::Bool,
        "google.protobuf.StringValue" => Scalar::String,
        "google.protobuf.BytesValue" => Scalar::Bytes,
        _ => return None,
    })
}

fn logical(message: &MessageDescriptor) -> Option<(&'static str, Scalar)> {
    Some(match message.full_name() {
        "confluent.type.Decimal" => (DECIMAL, Scalar::Bytes),
        "google.type.Date" => (DATE, Scalar::I32),
        "google.type.TimeOfDay" => (TIME, Scalar::I32),
        "google.protobuf.Timestamp" => (TIMESTAMP, Scalar::I64),
        _ => return None,
    })
}

fn field_meta(field: &FieldDescriptor) -> Option<ProtoValue> {
    let extension = field
        .parent_pool()
        .get_extension_by_name("confluent.field_meta")?;
    let options = field.options();
    if !options.has_extension(&extension) {
        return None;
    }
    let meta = options.get_extension(&extension);
    Some(meta.as_message()?.get_field_by_name("params")?.into_owned())
}

fn meta_param<'a>(params: &'a Option<ProtoValue>, key: &str) -> Option<&'a str> {
    let ProtoValue::Map(params) = params.as_ref()? else {
        return None;
    };
    params.get(&MapKey::String(key.into()))?.as_str()
}

fn map_fields(
    field: &FieldDescriptor,
    message: &MessageDescriptor,
) -> Option<(FieldDescriptor, FieldDescriptor)> {
    if field.cardinality() != Cardinality::Repeated || !message.name().ends_with("Entry") {
        return None;
    }
    let mut fields = declared_fields(message);
    let key = fields.next()?;
    let value = fields.next()?;
    if key.name() == "key"
        && value.name() == "value"
        && fields.next().is_none()
        && key.cardinality() != Cardinality::Repeated
        && value.cardinality() != Cardinality::Repeated
    {
        Some((key, value))
    } else {
        None
    }
}

fn map_name(name: &str) -> String {
    let (prefix, simple) = name.rsplit_once('.').map_or(("", name), |(prefix, tail)| {
        (&name[..prefix.len() + 1], tail)
    });
    let mut result = prefix.to_owned();
    for (index, byte) in simple
        .strip_suffix("Entry")
        .unwrap_or(simple)
        .bytes()
        .enumerate()
    {
        if index > 0 && byte.is_ascii_uppercase() {
            result.push('_');
        }
        result.push(char::from(byte.to_ascii_lowercase()));
    }
    result
}

fn convert(
    raw: &ProtoValue,
    descriptor: &Kind,
    schema: &ConnectSchema,
    options: &Options,
    context: &mut ConversionContext,
    depth: usize,
) -> Result<Value> {
    check_depth(depth)?;
    let _scratch = if matches!(schema.kind, SchemaKind::Reference(_)) {
        Some(
            context
                .scratch_budget()
                .reserve(schema.view_allocation_bound())?,
        )
    } else {
        None
    };
    let schema = schema.resolved();
    match &schema.kind {
        SchemaKind::Array(element) => {
            let ProtoValue::List(values) = raw else {
                return Err(wrong_type());
            };
            context.reserve(
                values
                    .len()
                    .checked_mul(256)
                    .and_then(|bytes| bytes.checked_add(1024))
                    .ok_or_else(overflow)?,
            )?;
            let mut list = ListBuilder::with_capacity(values.len());
            for value in values {
                if !unknown_closed_enum(value, descriptor) {
                    list.push(convert(
                        value,
                        descriptor,
                        element,
                        options,
                        context,
                        depth + 1,
                    )?);
                }
            }
            Ok(Value::List(list.finish()))
        }
        SchemaKind::Map(key, value) => {
            let Kind::Message(entry) = descriptor else {
                return Err(wrong_type());
            };
            let key_field = entry.get_field_by_name("key").ok_or_else(wrong_type)?;
            let value_field = entry.get_field_by_name("value").ok_or_else(wrong_type)?;
            let mut entries = Vec::new();
            match raw {
                ProtoValue::Map(values) => {
                    context.reserve(
                        values
                            .len()
                            .checked_mul(512)
                            .and_then(|bytes| bytes.checked_add(1024))
                            .ok_or_else(overflow)?,
                    )?;
                    let mut ordered: Vec<_> = values.iter().collect();
                    ordered.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
                    for (raw_key, raw_value) in ordered {
                        if let MapKey::String(text) = raw_key {
                            context.reserve(text.len())?;
                        }
                        entries.push((
                            convert(
                                &raw_key.clone().into(),
                                &key_field.kind(),
                                key,
                                options,
                                context,
                                depth + 1,
                            )?,
                            convert(
                                raw_value,
                                &value_field.kind(),
                                value,
                                options,
                                context,
                                depth + 1,
                            )?,
                        ));
                    }
                }
                ProtoValue::List(values) => {
                    context.reserve(
                        values
                            .len()
                            .checked_mul(512)
                            .and_then(|bytes| bytes.checked_add(1024))
                            .ok_or_else(overflow)?,
                    )?;
                    for item in values {
                        let item = item.as_message().ok_or_else(wrong_type)?;
                        let key = convert(
                            &item.get_field(&key_field),
                            &key_field.kind(),
                            key,
                            options,
                            context,
                            depth + 1,
                        )?;
                        let value = convert(
                            &item.get_field(&value_field),
                            &value_field.kind(),
                            value,
                            options,
                            context,
                            depth + 1,
                        )?;
                        if let Some((_, old)) =
                            entries.iter_mut().find(|(existing, _)| existing == &key)
                        {
                            *old = value;
                        } else {
                            entries.push((key, value));
                        }
                    }
                }
                _ => return Err(wrong_type()),
            }
            Ok(Value::Map {
                entries,
                sorted: false,
            })
        }
        SchemaKind::Struct(fields) => {
            let message = raw.as_message().ok_or_else(wrong_type)?;
            let descriptor = message.descriptor();
            context.reserve(struct_bound(fields.iter().map(|field| field.name.len()))?)?;
            let mut result = StructFields::new();
            for field in fields {
                let value = if let Some(field_descriptor) =
                    descriptor.get_field_by_name(&field.name)
                    && (options.flatten_unions || real_oneof(&field_descriptor).is_none())
                {
                    field_value(
                        message,
                        &field_descriptor,
                        &field.schema,
                        options,
                        context,
                        depth + 1,
                    )?
                } else {
                    let union = descriptor
                        .oneofs()
                        .filter(|union| !union.is_synthetic())
                        .find(|union| options.union_name(union) == field.name)
                        .ok_or_else(wrong_type)?;
                    union_value(message, &union, &field.schema, options, context, depth + 1)?
                };
                result.insert(field.name.clone(), value);
            }
            Ok(Value::Struct(result))
        }
        kind => {
            if let ProtoValue::Message(message) = raw {
                if let Some(name @ (DATE | TIME | TIMESTAMP | DECIMAL)) = schema.name.as_deref() {
                    return logical_value(name, message, context);
                }
                let field = message
                    .descriptor()
                    .get_field_by_name("value")
                    .ok_or_else(wrong_type)?;
                return primitive(&message.get_field(&field), &field.kind(), kind, context);
            }
            primitive(raw, descriptor, kind, context)
        }
    }
}

fn field_value(
    message: &DynamicMessage,
    descriptor: &FieldDescriptor,
    schema: &ConnectSchema,
    options: &Options,
    context: &mut ConversionContext,
    depth: usize,
) -> Result<Value> {
    let optional = descriptor.field_descriptor_proto().proto3_optional()
        || options.proto2_optional && has_optional_keyword(descriptor);
    let kind = descriptor.kind();
    let in_oneof = real_oneof(descriptor).is_some();
    let present = message.has_field(descriptor);
    if !present
        && (in_oneof
            || (!options.generate_absent_messages
                && (optional
                    || (matches!(kind, Kind::Message(_))
                        && descriptor.cardinality() != Cardinality::Repeated))))
    {
        return Ok(Value::Null);
    }
    let raw = message.get_field(descriptor);
    if unknown_closed_enum(&raw, &kind) {
        if in_oneof || (optional && !options.generate_absent_messages) {
            return Ok(Value::Null);
        }
        return convert(
            &descriptor.default_value(),
            &kind,
            schema,
            options,
            context,
            depth,
        );
    }
    convert(&raw, &kind, schema, options, context, depth)
}

fn union_value(
    message: &DynamicMessage,
    union: &OneofDescriptor,
    schema: &ConnectSchema,
    options: &Options,
    context: &mut ConversionContext,
    depth: usize,
) -> Result<Value> {
    let Some(active) = union.fields().find(|field| message.has_field(field)) else {
        return Ok(Value::Null);
    };
    let _scratch = context
        .scratch_budget()
        .reserve(schema.view_allocation_bound())?;
    let schema = schema.resolved();
    let SchemaKind::Struct(members) = &schema.kind else {
        return Err(wrong_type());
    };
    let mut result = StructFields::new();
    context.reserve(struct_bound(
        members.iter().map(|member| member.name.len()),
    )?)?;
    for member in members {
        let value = if member.name == active.name() {
            let value = field_value(
                message,
                &active,
                &member.schema,
                options,
                context,
                depth + 1,
            )?;
            if value == Value::Null {
                return Ok(Value::Null);
            }
            value
        } else {
            Value::Null
        };
        result.insert(member.name.clone(), value);
    }
    Ok(Value::Struct(result))
}

fn unknown_closed_enum(raw: &ProtoValue, kind: &Kind) -> bool {
    matches!((raw, kind), (ProtoValue::EnumNumber(number), Kind::Enum(enumeration))
        if enumeration.parent_file().syntax() == Syntax::Proto2 && enumeration.get_value(*number).is_none())
}

fn primitive(
    raw: &ProtoValue,
    descriptor: &Kind,
    schema: &SchemaKind,
    context: &mut ConversionContext,
) -> Result<Value> {
    Ok(match (raw, schema) {
        (ProtoValue::Bool(value), SchemaKind::Bool) => Value::Bool(*value),
        (ProtoValue::I32(value), SchemaKind::I8) => Value::I8(*value as i8),
        (ProtoValue::I32(value), SchemaKind::I16) => Value::I16(*value as i16),
        (ProtoValue::I32(value) | ProtoValue::EnumNumber(value), SchemaKind::I32) => {
            Value::I32(*value)
        }
        (ProtoValue::I64(value), SchemaKind::I64) => Value::I64(*value),
        (ProtoValue::U32(value), SchemaKind::I64) => Value::I64(i64::from(*value)),
        (ProtoValue::U64(value), SchemaKind::I64) => Value::I64(*value as i64),
        (ProtoValue::F32(value), SchemaKind::F32) => Value::F32(*value),
        (ProtoValue::F64(value), SchemaKind::F64) => Value::F64(*value),
        (ProtoValue::String(value), SchemaKind::String) => {
            context.reserve(value.len())?;
            Value::Utf8(value.clone())
        }
        (ProtoValue::Bytes(value), SchemaKind::Bytes) => {
            context.reserve(value.len())?;
            Value::Binary(value.to_vec())
        }
        (ProtoValue::EnumNumber(value), SchemaKind::String) => {
            let Kind::Enum(enumeration) = descriptor else {
                return Err(wrong_type());
            };
            if let Some(symbol) = enumeration.get_value(*value) {
                context.reserve(symbol.name().len())?;
                Value::Utf8(symbol.name().to_owned())
            } else {
                context.reserve(
                    enumeration
                        .name()
                        .len()
                        .checked_add(32)
                        .ok_or_else(overflow)?,
                )?;
                Value::Utf8(format!("UNKNOWN_ENUM_VALUE_{}_{value}", enumeration.name()))
            }
        }
        _ => return Err(wrong_type()),
    })
}

fn logical_value(
    name: &str,
    message: &DynamicMessage,
    context: &mut ConversionContext,
) -> Result<Value> {
    let integer = |name| {
        message
            .get_field_by_name(name)
            .and_then(|value| value.as_i64().or_else(|| value.as_i32().map(i64::from)))
            .ok_or_else(wrong_type)
    };
    let invalid = || error("conversion-error", "Invalid Protobuf logical value");
    let millis = match name {
        DATE => {
            use rtdb_types::java_date::{
                CUTOVER_DAY, MILLIS_PER_DAY, date_from_epoch_day, gregorian_day, julian_day,
            };
            let year = i32::try_from(integer("year")?).map_err(|_| invalid())?;
            let month = u8::try_from(integer("month")?).map_err(|_| invalid())?;
            let day = u8::try_from(integer("day")?).map_err(|_| invalid())?;
            if year <= 0 || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
                return Err(invalid());
            }
            let gregorian = gregorian_day(year, month, day);
            let epoch_day = if gregorian >= CUTOVER_DAY {
                gregorian
            } else {
                julian_day(year, month, day)
            };
            if date_from_epoch_day(epoch_day) != (year, month, day) {
                return Err(invalid());
            }
            epoch_day.checked_mul(MILLIS_PER_DAY).ok_or_else(invalid)?
        }
        TIME => {
            let (hours, minutes, seconds, nanos) = (
                integer("hours")?,
                integer("minutes")?,
                integer("seconds")?,
                integer("nanos")?,
            );
            if !(0..24).contains(&hours)
                || !(0..60).contains(&minutes)
                || !(0..60).contains(&seconds)
                || !(0..1_000_000_000).contains(&nanos)
            {
                return Err(invalid());
            }
            (hours * 3600 + minutes * 60 + seconds) * 1000 + nanos / 1_000_000
        }
        TIMESTAMP => {
            let (seconds, nanos) = (integer("seconds")?, integer("nanos")?);
            if !(-62_135_596_800..=253_402_300_799).contains(&seconds)
                || !(0..1_000_000_000).contains(&nanos)
            {
                return Err(invalid());
            }
            seconds * 1000 + nanos / 1_000_000
        }
        DECIMAL => {
            let raw = message.get_field_by_name("value").ok_or_else(wrong_type)?;
            let bytes = raw
                .as_bytes()
                .filter(|bytes| !bytes.is_empty())
                .ok_or_else(invalid)?;
            context.reserve(
                bytes
                    .len()
                    .checked_mul(8)
                    .and_then(|bytes| bytes.checked_add(256))
                    .ok_or_else(overflow)?,
            )?;
            let coefficient = num_bigint::BigInt::from_signed_bytes_be(bytes);
            let digits = coefficient.to_str_radix(10).trim_start_matches('-').len();
            let (precision, bit_width) = match digits {
                0..=32 => (32, 128),
                33..=64 => (64, 256),
                _ => return Err(invalid()),
            };
            let signed = coefficient.to_signed_bytes_be();
            let width = usize::from(bit_width / 8);
            let mut sign_extended = vec![
                if coefficient.sign() == num_bigint::Sign::Minus {
                    255
                } else {
                    0
                };
                width
            ];
            sign_extended[width - signed.len()..].copy_from_slice(&signed);
            let scale = i32::try_from(integer("scale")?).map_err(|_| invalid())?;
            return Ok(Value::Decimal(DecimalValue::new(
                sign_extended,
                precision,
                scale,
                bit_width,
            )?));
        }
        _ => return Err(wrong_type()),
    };
    Ok(date_value(millis))
}

pub(super) fn validate_required(message: &DynamicMessage) -> Result<()> {
    for field in message
        .descriptor()
        .fields()
        .filter(FieldDescriptor::is_required)
    {
        if !message.has_field(&field)
            || unknown_closed_enum(&message.get_field(&field), &field.kind())
        {
            return Err(error(
                "conversion-error",
                format!("Protobuf required field '{}' is missing", field.full_name()),
            ));
        }
    }
    for (_, value) in message.fields() {
        match value {
            ProtoValue::Message(child) => validate_required(child)?,
            ProtoValue::List(values) => {
                for child in values.iter().filter_map(ProtoValue::as_message) {
                    validate_required(child)?;
                }
            }
            ProtoValue::Map(values) => {
                for child in values.values().filter_map(ProtoValue::as_message) {
                    validate_required(child)?;
                }
            }
            _ => (),
        }
    }
    Ok(())
}

fn struct_bound(names: impl Iterator<Item = usize>) -> Result<usize> {
    names
        .into_iter()
        .try_fold(1024usize, |bound, len| {
            bound.checked_add(len)?.checked_add(512)
        })
        .ok_or_else(overflow)
}

fn check_depth(depth: usize) -> Result<()> {
    if depth > MAX_DEPTH {
        return Err(error(
            "conversion-error",
            "Protobuf Connect projection nesting limit exceeded",
        ));
    }
    Ok(())
}

fn wrong_type() -> rtdb_types::Anomaly {
    error(
        "conversion-error",
        "Protobuf value does not match its Connect schema",
    )
}
