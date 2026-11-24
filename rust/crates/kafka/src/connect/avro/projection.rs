use super::{
    Avro, AvroDatumPlan, ConnectSchema, SchemaKind, StructFields, datum, error, graph, resolve,
    union::{Matcher, Selected},
    wire::{Node, Tape},
};
use apache_avro::Schema;
use rtdb_types::{ListBuilder, Result, Value};
use std::{collections::BTreeMap, sync::Arc};

pub(super) struct Projection {
    steps: Vec<Step>,
    root: usize,
}

struct Step {
    shape: Shape,
    logical: Logical,
}

enum Shape {
    Scalar(Scalar),
    Record(Vec<(String, usize)>),
    Entry(Vec<(String, usize)>),
    Array {
        child: usize,
        packed: bool,
    },
    Entries(usize),
    Map(usize),
    Optional(Vec<Option<usize>>),
    Union {
        tags: Vec<Option<Result<UnionTag>>>,
        candidates: Vec<Result<Candidate>>,
        fields: Vec<String>,
    },
    Invalid(&'static str),
}

struct UnionTag {
    selected: Selected,
    schema: Arc<ConnectSchema>,
}

struct Candidate {
    matcher: Matcher,
    source: Arc<ConnectSchema>,
    output: Result<UnionCase>,
}

struct UnionCase {
    selected: usize,
    destination: usize,
    target: Arc<ConnectSchema>,
}

#[derive(Clone, Copy)]
enum Scalar {
    Bool,
    I8,
    I16,
    I32,
    I64,
    F32,
    F64,
    String,
    Bytes,
}

enum Logical {
    Plain,
    Date,
    Time,
    Timestamp,
    Decimal(Option<String>),
    InvalidDate,
}

impl Projection {
    pub(super) fn compile(schema: &Schema, connect: &ConnectSchema, datum: &AvroDatumPlan) -> Self {
        let mut compiler = Compiler {
            steps: Vec::new(),
            seen: BTreeMap::new(),
            datum,
        };
        let root = compiler.compile(schema, connect, 0);
        Self {
            steps: compiler.steps,
            root,
        }
    }

    pub(super) fn project(&self, tape: &mut Tape, root: usize) -> Result<Value> {
        self.value(tape, root, self.root, 0)
    }

    fn value(&self, tape: &mut Tape, id: usize, step: usize, depth: usize) -> Result<Value> {
        if depth > 128 {
            return Err(error("avro-decode", "Avro datum nesting limit exceeded"));
        }
        if matches!(tape.node(id), Node::Scalar(Avro::Null)) {
            return Ok(Value::Null);
        }
        let Step { shape, logical } = &self.steps[step];
        let value = match (shape, tape.take(id)) {
            (Shape::Scalar(kind), Node::Scalar(value)) => kind.value(value, logical)?,
            (Shape::Record(children) | Shape::Entry(children), Node::Record(fields)) => {
                let mut result = StructFields::new();
                for ((name, id), (_, child)) in fields.into_iter().zip(children) {
                    result.insert(name, self.value(tape, id, *child, depth + 1)?);
                }
                Value::Struct(result)
            }
            (Shape::Array { child, packed }, Node::Array(values)) => {
                let output = values
                    .into_iter()
                    .map(|id| self.value(tape, id, *child, depth + 1));
                if *packed {
                    let mut list = ListBuilder::with_capacity(output.len());
                    for value in output {
                        list.push(value?);
                    }
                    Value::List(list.finish())
                } else {
                    Value::list(output.collect::<Result<_>>()?)
                }
            }
            (Shape::Entries(child), Node::Array(values)) => {
                let entries = values
                    .into_iter()
                    .map(|id| self.entry(tape, id, *child, depth + 1))
                    .collect::<Result<_>>()?;
                Value::Map {
                    entries,
                    sorted: false,
                }
            }
            (Shape::Map(child), Node::Map(entries)) => {
                let entries = entries
                    .ordered()?
                    .into_iter()
                    .map(|(key, id)| {
                        Ok((
                            super::MapKey::String(key.into_text()),
                            self.value(tape, id, *child, depth + 1)?,
                        ))
                    })
                    .collect::<Result<Vec<_>>>()?;
                let order = datum::string_map_order(&entries)?;
                let mut entries = entries.into_iter().map(Some).collect::<Vec<_>>();
                Value::Map {
                    entries: order
                        .into_iter()
                        .map(|index| {
                            let (key, value) =
                                entries[index].take().expect("unique converted map key");
                            (Value::Utf8(key.into_text()), value)
                        })
                        .collect(),
                    sorted: false,
                }
            }
            (Shape::Optional(children), Node::Union(tag, id)) => {
                return match children
                    .get(tag as usize)
                    .ok_or_else(|| error("avro-decode", "Invalid union member"))?
                {
                    None if matches!(tape.node(id), Node::Scalar(Avro::Null)) => Ok(Value::Null),
                    None => Err(error("avro-decode", "Invalid union member")),
                    Some(child) => self.value(tape, id, *child, depth + 1),
                };
            }
            (
                Shape::Union {
                    tags,
                    candidates,
                    fields,
                },
                Node::Union(tag, id),
            ) => {
                if matches!(tape.node(id), Node::Scalar(Avro::Null)) {
                    return Ok(Value::Null);
                }
                let tag = tags
                    .get(tag as usize)
                    .ok_or_else(|| error("avro-decode", "Invalid union member"))?
                    .as_ref()
                    .ok_or_else(|| error("avro-decode", "Selected union schema field is absent"))?
                    .as_ref()
                    .map_err(Clone::clone)?;
                for candidate in candidates {
                    let candidate = candidate.as_ref().map_err(Clone::clone)?;
                    if !candidate.matcher.matches(
                        tape.node(id).probe(),
                        &candidate.source,
                        &tag.selected,
                        &tag.schema,
                    )? {
                        continue;
                    }
                    let case = candidate.output.as_ref().map_err(Clone::clone)?;
                    let value = self.value(tape, id, case.selected, depth + 1)?;
                    super::union::validate_destination(
                        &value,
                        &candidate.source,
                        &case.target,
                        depth + 1,
                    )?;
                    let mut result = fields
                        .iter()
                        .map(|name| (name.clone(), Value::Null))
                        .collect::<StructFields<_>>();
                    result.insert(fields[case.destination].clone(), value);
                    return Ok(Value::Struct(result));
                }
                return Err(error(
                    "avro-decode",
                    "No Connect union member matches the Avro value",
                ));
            }
            (Shape::Invalid(message), _) => return Err(error("avro-decode", *message)),
            _ => {
                return Err(error(
                    "avro-decode",
                    "Resolved value does not match the selected Connect output type",
                ));
            }
        };
        logical.convert(value)
    }

    fn entry(
        &self,
        tape: &mut Tape,
        id: usize,
        step: usize,
        depth: usize,
    ) -> Result<(Value, Value)> {
        if depth > 128 {
            return Err(error("avro-decode", "Avro datum nesting limit exceeded"));
        }
        let Step {
            shape: Shape::Entry(children),
            logical,
        } = &self.steps[step]
        else {
            return Err(error("avro-decode", "MapEntry value is not a record"));
        };
        let Node::Record(fields) = tape.take(id) else {
            return Err(error("avro-decode", "MapEntry value is not a record"));
        };
        let (mut key, mut value) = (None, None);
        for ((name, id), (_, child)) in fields.into_iter().zip(children) {
            let output = self.value(tape, id, *child, depth + 1)?;
            match name.as_str() {
                "key" => key = Some(output),
                "value" => value = Some(output),
                _ => unreachable!("admitted MapEntry field"),
            }
        }
        logical.check_container()?;
        Ok((
            key.ok_or_else(|| error("avro-decode", "MapEntry key is absent"))?,
            value.ok_or_else(|| error("avro-decode", "MapEntry value is absent"))?,
        ))
    }
}

impl Scalar {
    fn value(self, value: Avro, logical: &Logical) -> Result<Value> {
        Ok(match (self, value) {
            (Self::Bool, Avro::Boolean(value)) => Value::Bool(value),
            (Self::I8, Avro::Int(value)) if matches!(logical, Logical::Plain) => {
                Value::I8(value as i8)
            }
            (Self::I16, Avro::Int(value)) if matches!(logical, Logical::Plain) => {
                Value::I16(value as i16)
            }
            (Self::I8 | Self::I16 | Self::I32, Avro::Int(value)) => Value::I32(value),
            (Self::I64, Avro::Long(value)) => Value::I64(value),
            (Self::F32, Avro::Float(value)) => Value::F32(value),
            (Self::F64, Avro::Double(value)) => Value::F64(value),
            (Self::String, Avro::String(value) | Avro::Enum(_, value)) => Value::Utf8(value),
            (Self::String, Avro::Utf8(value)) => Value::Utf8(datum::java_utf8(&value)),
            (Self::Bytes, Avro::Bytes(value) | Avro::Fixed(_, value)) => Value::Binary(value),
            _ => {
                return Err(error(
                    "avro-decode",
                    "Resolved value does not match the selected Connect output type",
                ));
            }
        })
    }
}

impl Logical {
    fn compile(schema: &ConnectSchema) -> Self {
        match (schema.name.as_deref(), &schema.kind) {
            (Some("org.apache.kafka.connect.data.Decimal"), _) => {
                Self::Decimal(schema.parameters.get("scale").cloned())
            }
            (Some("org.apache.kafka.connect.data.Date"), SchemaKind::I32) => Self::Date,
            (Some("org.apache.kafka.connect.data.Time"), SchemaKind::I32) => Self::Time,
            (Some("org.apache.kafka.connect.data.Timestamp"), SchemaKind::I64) => Self::Timestamp,
            (
                Some(
                    "org.apache.kafka.connect.data.Date"
                    | "org.apache.kafka.connect.data.Time"
                    | "org.apache.kafka.connect.data.Timestamp",
                ),
                _,
            ) => Self::InvalidDate,
            _ => Self::Plain,
        }
    }

    fn convert(&self, value: Value) -> Result<Value> {
        Ok(match (self, value) {
            (Self::Plain, value) => value,
            (Self::Decimal(scale), Value::Binary(bytes)) => {
                super::decimal_value_with_scale(bytes, scale.as_deref())?
            }
            (Self::Decimal(_), _) => {
                return Err(error(
                    "avro-decode",
                    "Logical Decimal value has an incompatible primitive representation",
                ));
            }
            (Self::Date, Value::I32(value)) => {
                super::super::data::date_value(i64::from(value) * 86_400_000)
            }
            (Self::Time, Value::I32(value)) => {
                if !(0..=86_400_000).contains(&value) {
                    return Err(error("avro-decode", "Time is outside one day"));
                }
                super::super::data::date_value(i64::from(value))
            }
            (Self::Timestamp, Value::I64(value)) => super::super::data::date_value(value),
            _ => {
                return Err(error(
                    "avro-decode",
                    "Logical date value has an incompatible primitive representation",
                ));
            }
        })
    }

    fn check_container(&self) -> Result<()> {
        if matches!(self, Self::Plain) {
            Ok(())
        } else {
            self.convert(Value::Null).map(drop)
        }
    }
}

struct Compiler<'a> {
    steps: Vec<Step>,
    seen: BTreeMap<(usize, (usize, usize)), usize>,
    datum: &'a AvroDatumPlan,
}
impl Compiler<'_> {
    fn compile(&mut self, schema: &Schema, connect: &ConnectSchema, depth: usize) -> usize {
        let datum = self.datum;
        let schema = match resolve(schema, &datum.names) {
            Ok(schema) => schema,
            Err(_) => return self.invalid("Unresolved schema reference"),
        };
        let key = match &connect.kind {
            SchemaKind::Reference(reference) => {
                (schema as *const Schema as usize, reference.identity())
            }
            _ => (
                schema as *const Schema as usize,
                (0, connect as *const ConnectSchema as usize),
            ),
        };
        if let Some(id) = self.seen.get(&key) {
            return *id;
        }
        if depth > 256 {
            return self.invalid("Avro datum nesting limit exceeded");
        }
        let id = self.invalid("Unresolved schema reference");
        self.seen.insert(key, id);
        let connect_view = connect.resolved();
        let shape = self.shape(schema, connect, &connect_view, depth);
        self.steps[id] = Step {
            shape,
            logical: Logical::compile(&connect_view),
        };
        id
    }

    fn invalid(&mut self, message: &'static str) -> usize {
        let id = self.steps.len();
        self.steps.push(Step {
            shape: Shape::Invalid(message),
            logical: Logical::Plain,
        });
        id
    }

    fn shape(
        &mut self,
        schema: &Schema,
        connect: &ConnectSchema,
        view: &ConnectSchema,
        depth: usize,
    ) -> Shape {
        let datum = self.datum;
        match (schema, &view.kind) {
            (Schema::Union(union), _) if is_flattened(union.variants(), datum.unions) => {
                Shape::Optional(
                    union
                        .variants()
                        .iter()
                        .map(|schema| {
                            (!matches!(schema, Schema::Null))
                                .then(|| self.compile(schema, connect, depth + 1))
                        })
                        .collect(),
                )
            }
            (Schema::Union(union), SchemaKind::Struct(fields)) => {
                let variants = union.variants();
                let options = datum.unions;
                let positions: BTreeMap<_, _> = fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| (field.name.as_str(), index))
                    .collect();
                let tags = variants
                    .iter()
                    .enumerate()
                    .map(|(tag, selected)| {
                        if matches!(selected, Schema::Null) {
                            return None;
                        }
                        Some((|| {
                            let selected = resolve(selected, &datum.names)?;
                            let selected_name = options.field_name(selected, tag);
                            let position = positions
                                .get(selected_name.as_str())
                                .copied()
                                .ok_or_else(|| {
                                    error("avro-decode", "Selected union schema field is absent")
                                })?;
                            let field = &fields[position];
                            Ok(UnionTag {
                                selected: Selected::new(selected, options),
                                schema: field.schema.clone(),
                            })
                        })())
                    })
                    .collect();
                let candidates = fields
                    .iter()
                    .zip(
                        variants
                            .iter()
                            .filter(|schema| !matches!(schema, Schema::Null)),
                    )
                    .enumerate()
                    .map(|(index, (field, member))| {
                        let member = resolve(member, &datum.names)?;
                        let candidate = field.schema.resolved();
                        let matcher = Matcher::compile(&candidate, options, index);
                        let output = (|| {
                            let destination_name =
                                super::union::field_name(&candidate, options, index)?;
                            let destination = positions
                                .get(destination_name.as_str())
                                .copied()
                                .ok_or_else(|| {
                                    error(
                                        "avro-decode",
                                        "Converted union field is absent from its schema",
                                    )
                                })?;
                            let selected = self.compile(member, &field.schema, depth + 1);
                            Ok(UnionCase {
                                selected,
                                destination,
                                target: fields[destination].schema.clone(),
                            })
                        })();
                        Ok(Candidate {
                            matcher,
                            source: field.schema.clone(),
                            output,
                        })
                    })
                    .collect();
                Shape::Union {
                    tags,
                    candidates,
                    fields: fields.iter().map(|field| field.name.clone()).collect(),
                }
            }
            (Schema::Union(_), _) => Shape::Invalid("Union Connect schema is not a Struct"),
            (Schema::Record(record), SchemaKind::Struct(fields)) => {
                let mut children = Vec::with_capacity(record.fields.len());
                for field in &record.fields {
                    let Some(child) = fields.iter().find(|child| child.name == field.name) else {
                        return Shape::Invalid("Record field Connect schema is absent");
                    };
                    children.push((
                        field.name.clone(),
                        self.compile(&field.schema, &child.schema, depth + 1),
                    ));
                }
                Shape::Record(children)
            }
            (Schema::Record(record), SchemaKind::Map(key, value)) => Shape::Entry(
                record
                    .fields
                    .iter()
                    .map(|field| {
                        let child = match field.name.as_str() {
                            "key" => self.compile(&field.schema, key, depth + 1),
                            "value" => self.compile(&field.schema, value, depth + 1),
                            _ => self.invalid("Record field Connect schema is absent"),
                        };
                        (field.name.clone(), child)
                    })
                    .collect(),
            ),
            (Schema::Record(_), _) => Shape::Invalid("Record field Connect schema is absent"),
            (Schema::Array(array), SchemaKind::Array(child)) => {
                let view = child.resolved();
                let packed = matches!(
                    view.kind,
                    SchemaKind::I8
                        | SchemaKind::I16
                        | SchemaKind::I32
                        | SchemaKind::I64
                        | SchemaKind::F32
                        | SchemaKind::F64
                ) && matches!(Logical::compile(&view), Logical::Plain);
                Shape::Array {
                    child: self.compile(&array.items, child, depth + 1),
                    packed,
                }
            }
            (Schema::Array(array), SchemaKind::Map(_, _)) => {
                Shape::Entries(self.compile(&array.items, connect, depth + 1))
            }
            (Schema::Array(_), _) => Shape::Invalid("Array Connect schema is absent"),
            (Schema::Map(map), SchemaKind::Map(_, child)) => {
                Shape::Map(self.compile(&map.types, child, depth + 1))
            }
            (Schema::Map(_), _) => Shape::Invalid("Map Connect schema is absent"),
            (_, kind) => Shape::Scalar(match kind {
                SchemaKind::Bool => Scalar::Bool,
                SchemaKind::I8 => Scalar::I8,
                SchemaKind::I16 => Scalar::I16,
                SchemaKind::I32 => Scalar::I32,
                SchemaKind::I64 => Scalar::I64,
                SchemaKind::F32 => Scalar::F32,
                SchemaKind::F64 => Scalar::F64,
                SchemaKind::String => Scalar::String,
                SchemaKind::Bytes => Scalar::Bytes,
                _ => {
                    return Shape::Invalid(
                        "Resolved value does not match the selected Connect output type",
                    );
                }
            }),
        }
    }
}

pub(super) fn is_flattened(variants: &[Schema], options: graph::UnionOptions) -> bool {
    (variants.len() == 1 && options.flatten_singleton)
        || (variants.len() == 2 && variants.iter().any(|schema| matches!(schema, Schema::Null)))
}
