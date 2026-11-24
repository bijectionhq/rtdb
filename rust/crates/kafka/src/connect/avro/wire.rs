use super::{
    Avro, AvroDatumPlan, AvroMap, BinaryWindow, MapKey, Preflight, datum, error, resolution,
};
use rtdb_types::Result;
use std::sync::Arc;

pub(in crate::connect) struct WireDatum {
    pub(super) plan: Arc<AvroDatumPlan>,
    pub(super) tape: Tape,
    pub(super) root: usize,
}

#[derive(Default)]
pub(super) struct Tape(Vec<Entry>);

struct Entry {
    node: Node,
    bytes: Option<usize>,
    depth: usize,
}

pub(super) enum Node {
    Scalar(Avro),
    Record(Vec<(String, usize)>),
    Array(Vec<usize>),
    Map(AvroMap<usize>),
    Union(u32, usize),
}

impl Node {
    pub(super) fn probe(&self) -> super::union::Probe {
        use super::union::Probe;
        match self {
            Self::Scalar(value) => value.into(),
            Self::Record(_) => Probe::Record,
            Self::Array(_) => Probe::Array,
            Self::Map(_) => Probe::Map,
            Self::Union(..) => Probe::Other,
        }
    }
}

impl WireDatum {
    pub(in crate::connect) fn is_null(&self) -> bool {
        self.tape.is_null(self.root)
    }

    // String writers expose promoted byte buffers through Java's ByteBuffer text.
    pub(super) fn string_result(
        &mut self,
        writer: &Arc<AvroDatumPlan>,
        context: &mut super::ConversionContext,
    ) -> Result<()> {
        writer.retain_source(context)?;
        self.plan.retain_source(context)?;
        if matches!(
            super::resolve(&writer.root, &writer.names)?,
            apache_avro::Schema::String
        ) {
            let mut id = self.root;
            while let Node::Union(_, child) = self.tape.node(id) {
                id = *child;
            }
            let text = match self.tape.take(id) {
                Node::Scalar(Avro::String(value)) => value,
                Node::Scalar(Avro::Utf8(value)) => {
                    context.reserve(
                        value
                            .len()
                            .checked_mul(6)
                            .and_then(|bytes| bytes.checked_add(4096))
                            .ok_or_else(resolution::overflow)?,
                    )?;
                    datum::java_utf8(&value)
                }
                Node::Scalar(Avro::Bytes(value)) => {
                    context.reserve(4096)?;
                    format!(
                        "java.nio.HeapByteBuffer[pos=0 lim={} cap={}]",
                        value.len(),
                        value.len()
                    )
                }
                _ => {
                    return Err(error(
                        "avro-decode",
                        "String writer resolved to an invalid output",
                    ));
                }
            };
            self.root = datum::BuildDatum::scalar(&mut self.tape, Avro::String(text));
            self.plan = Arc::clone(writer);
        }
        Ok(())
    }
}

impl Tape {
    pub(super) fn node(&self, index: usize) -> &Node {
        &self.0[index].node
    }
    pub(super) fn take(&mut self, index: usize) -> Node {
        std::mem::replace(&mut self.0[index].node, Node::Scalar(Avro::Null))
    }

    pub(super) fn is_null(&self, mut index: usize) -> bool {
        while let Node::Union(_, child) = self.node(index) {
            index = *child;
        }
        matches!(self.node(index), Node::Scalar(Avro::Null))
    }

    pub(super) fn bound(&self, index: usize, level: usize) -> Result<usize> {
        let entry = &self.0[index];
        if level + entry.depth > 128 {
            return Err(error(
                "avro-resolution",
                "Avro resolution nesting limit exceeded",
            ));
        }
        entry.bytes.ok_or_else(resolution::overflow)
    }

    fn append(
        &mut self,
        node: Node,
        own: Option<usize>,
        children: impl IntoIterator<Item = usize>,
    ) -> usize {
        let mut bytes = own.and_then(|own| own.checked_add(4096));
        let mut depth = 0;
        for child in children {
            let entry = &self.0[child];
            depth = depth.max(1 + entry.depth);
            bytes = bytes.and_then(|bytes| bytes.checked_add(entry.bytes?));
        }
        let index = self.0.len();
        self.0.push(Entry { node, bytes, depth });
        index
    }

    pub(super) fn replace(&mut self, index: usize, node: Node) {
        self.0[index].node = node;
    }

    pub(super) fn physical(&mut self, value: Avro) -> usize {
        use datum::BuildDatum;
        match value {
            Avro::Record(fields) => {
                let fields = fields
                    .into_iter()
                    .map(|(name, value)| (name, self.physical(value)))
                    .collect();
                self.record(fields)
            }
            Avro::Array(values) => {
                let values = values
                    .into_iter()
                    .map(|value| self.physical(value))
                    .collect();
                self.array(values)
            }
            Avro::Map(AvroMap::Insertions {
                first_block,
                entries,
                strings,
            }) => {
                let entries = entries
                    .into_iter()
                    .map(|(key, value)| (key, self.physical(value)))
                    .collect();
                self.map(first_block, entries, strings)
            }
            Avro::Map(AvroMap::Ordered(entries)) => {
                let entries: Vec<_> = entries
                    .into_iter()
                    .map(|(key, value)| (key, self.physical(value)))
                    .collect();
                self.ordered_map(entries)
            }
            Avro::Union(tag, value) => {
                let child = self.physical(*value);
                self.union(tag, child)
            }
            value => self.scalar(value),
        }
    }

    fn ordered_map(&mut self, entries: Vec<(MapKey, usize)>) -> usize {
        let bytes = entries.iter().try_fold(0usize, |size, (key, _)| {
            size.checked_add(key.len().checked_mul(8)?)
        });
        let slot = self.append(
            Node::Scalar(Avro::Null),
            bytes,
            entries.iter().map(|(_, child)| *child),
        );
        self.replace(slot, Node::Map(AvroMap::Ordered(entries)));
        slot
    }
}

impl datum::BuildDatum for Tape {
    type Node = usize;

    fn scalar(&mut self, value: Avro) -> usize {
        let bytes = match &value {
            Avro::String(value) => value.len().checked_mul(4),
            Avro::Bytes(value) | Avro::Utf8(value) | Avro::Fixed(_, value) => {
                value.len().checked_mul(4)
            }
            Avro::Enum(_, value) => Some(value.len()),
            _ => Some(0),
        };
        self.append(Node::Scalar(value), bytes, [])
    }
    fn record(&mut self, fields: Vec<(String, usize)>) -> usize {
        let bytes = fields
            .iter()
            .try_fold(0usize, |size, (name, _)| size.checked_add(name.len()));
        let slot = self.append(
            Node::Scalar(Avro::Null),
            bytes,
            fields.iter().map(|(_, child)| *child),
        );
        self.0[slot].node = Node::Record(fields);
        slot
    }
    fn array(&mut self, values: Vec<usize>) -> usize {
        let slot = self.append(Node::Scalar(Avro::Null), Some(0), values.iter().copied());
        self.0[slot].node = Node::Array(values);
        slot
    }
    fn map(&mut self, first_block: usize, entries: Vec<(MapKey, usize)>, strings: bool) -> usize {
        let bytes = entries.iter().try_fold(0usize, |size, (key, _)| {
            size.checked_add(key.len().checked_mul(8)?)
        });
        let slot = self.append(
            Node::Scalar(Avro::Null),
            bytes,
            entries.iter().map(|(_, child)| *child),
        );
        self.0[slot].node = Node::Map(AvroMap::new(first_block, entries, strings));
        slot
    }
    fn union(&mut self, index: u32, child: usize) -> usize {
        self.append(Node::Union(index, child), Some(0), [child])
    }
    fn reader_default(&mut self, value: &Avro) -> usize {
        match value {
            Avro::Record(fields) => {
                let fields = fields
                    .iter()
                    .map(|(name, value)| (name.clone(), self.reader_default(value)))
                    .collect();
                self.record(fields)
            }
            Avro::Array(values) => {
                let values = values.iter().map(|value| self.reader_default(value)).collect();
                self.array(values)
            }
            Avro::Map(map) => {
                let entries = map
                    .iter()
                    .map(|(key, value)| (key.clone(), self.reader_default(value)))
                    .collect();
                match map {
                    AvroMap::Insertions {
                        first_block,
                        strings,
                        ..
                    } => self.map(*first_block, entries, *strings),
                    AvroMap::Ordered(_) => self.ordered_map(entries),
                }
            }
            Avro::Union(tag, value) => {
                let child = self.reader_default(value);
                self.union(*tag, child)
            }
            value => self.scalar(value.clone()),
        }
    }
}

impl AvroDatumPlan {
    pub(in crate::connect) fn decode_wire(
        self: &Arc<Self>,
        bytes: &[u8],
        context: &mut super::ConversionContext,
    ) -> Result<WireDatum> {
        self.retain_source(context)?;
        let mut tape = Tape::default();
        let root = if self.raw_bytes() {
            context.reserve(
                bytes
                    .len()
                    .checked_mul(4)
                    .and_then(|bytes| bytes.checked_add(4096))
                    .ok_or_else(|| {
                        error("allocation-overflow", "Avro byte value allocation overflow")
                    })?,
            )?;
            datum::BuildDatum::scalar(&mut tape, Avro::Bytes(bytes.to_vec()))
        } else {
            let mut reader = Preflight {
                bytes,
                offset: 0,
                window: BinaryWindow::new(bytes.len()),
                allocated: 4096,
                limit: context.allocation_limit(),
                names: &self.names,
                view_bytes: 0,
            };
            reader.value(&self.root, 0)?;
            context.reserve(reader.allocated)?;
            reader.offset = 0;
            reader.window = BinaryWindow::new(bytes.len());
            datum::read_with(&self.root, &mut reader, 0, &mut tape)?
        };
        Ok(WireDatum {
            plan: Arc::clone(self),
            tape,
            root,
        })
    }
}
