/// A rejected wire representation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    /// The response version is outside 0..13.
    UnsupportedVersion,
    /// A length or fixed-width value exceeds the available bytes.
    Truncated,
    /// A non-nullable length, array count, tag count or tag size is negative.
    InvalidLength,
    /// A varint still continues after its fifth byte.
    InvalidVarint,
    /// A decoded string length exceeds 32767 bytes.
    StringTooLong,
    /// An array count exceeds 1,000,000 after the remaining-byte check.
    ArrayTooLong,
    /// A tag count exceeds 10,000 after the remaining-byte check.
    TooManyTags,
    /// A string is malformed UTF8 in the MetadataView facade; RawMetadataView preserves its bytes.
    UnsupportedUtf8,
}

/// Validated counts for caller-side result admission, with no retained output allocation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Summary {
    /// Number of broker entries, including repeated IDs.
    pub brokers: usize,
    /// Number of topic entries, including repeated names.
    pub topics: usize,
    /// Total partition entries across topics, in wire order.
    pub partitions: usize,
    /// Total wire payload bytes across all strings, before any replacement decoding.
    pub string_bytes: usize,
    /// Top-level Kafka error code; zero before Metadata v13.
    pub error: i16,
    /// Signed broker throttle time in milliseconds; zero before Metadata v3.
    pub throttle_ms: i32,
}

/// Borrowed broker fields in response order; IDs and ports retain signed wire values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Broker<'a> {
    /// Broker node ID.
    pub id: i32,
    /// Valid UTF8 hostname without normalization.
    pub host: &'a str,
    /// Advertised port.
    pub port: i32,
    /// Rack is absent before v1 and for a nullable wire value.
    pub rack: Option<&'a str>,
}

/// Borrowed topic fields preceding its partition events.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Topic<'a> {
    /// Signed Kafka topic error, including unknown codes.
    pub error: i16,
    /// Names may be absent beginning in v12.
    pub name: Option<&'a str>,
    /// Raw network-order UUID bytes; absent before v10.
    pub id: Option<&'a [u8; 16]>,
    /// The wire flag, false before v1.
    pub internal: bool,
    /// Number of following partition events for this topic.
    pub partitions: usize,
}

/// A borrowed network-order array of signed 32-bit integers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Int32Array<'a>(&'a [u8]);
impl<'a> Int32Array<'a> {
    /// Returns the element count, with no copy or sorting.
    pub fn len(self) -> usize { self.0.len() / 4 }
    /// Returns whether the array contains zero integers.
    pub fn is_empty(self) -> bool { self.0.is_empty() }
    /// Iterates signed values in their original order.
    pub fn iter(self) -> impl ExactSizeIterator<Item = i32> + 'a {
        self.0.chunks_exact(4).map(|bytes| i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }
}

/// Borrowed partition fields; no broker lookup or leader-availability filtering is applied.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Partition<'a> {
    /// Signed Kafka partition error, including unknown codes.
    pub error: i16,
    /// Partition index from the response, without sorting or renumbering.
    pub id: i32,
    /// Raw leader ID, including -1 for an absent leader.
    pub leader: i32,
    /// Raw epoch from v7 onward; reliability is reported separately by MetadataView.
    pub leader_epoch: Option<i32>,
    /// Replica IDs in wire order.
    pub replicas: Int32Array<'a>,
    /// In-sync replica IDs in wire order.
    pub isr: Int32Array<'a>,
    /// Offline replica IDs; empty before v5.
    pub offline: Int32Array<'a>,
}

/// Events preserve wire ordering and duplicates.
/// Each Topic is followed by its declared Partition events and one TopicEnd.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Event<'a> {
    /// Throttle duration in milliseconds; zero before v3.
    Throttle(i32),
    /// One advertised broker, including entries with duplicate node IDs.
    Broker(Broker<'a>),
    /// Cluster/controller values after all broker entries; -1 means no controller.
    Cluster {
        /// Nullable cluster ID; absent before v2.
        id: Option<&'a str>,
        /// Controller node ID; -1 before v1 or when absent.
        controller: i32,
    },
    /// Begins a topic group, including topics with no partitions.
    Topic(Topic<'a>),
    /// One partition of the current topic.
    Partition(Partition<'a>),
    /// Topic authorization bits; absent before v8.
    TopicEnd {
        /// Raw authorization bits; absent before v8.
        authorized_operations: Option<i32>,
    },
    /// Cluster authorization bits and raw top-level error, after all topics.
    End {
        /// Raw cluster authorization bits from v8..10.
        authorized_operations: Option<i32>,
        /// Raw signed top-level error; zero before v13.
        error: i16,
    },
}

/// A structurally validated borrowed Metadata body, excluding the Kafka response header.
/// Construction and visitation allocate no output storage and preserve the original order.
/// Unknown flexible tags and unconsumed suffixes remain in raw bytes; malformed UTF8 is explicitly unsupported.
#[derive(Clone, Copy, Debug)]
pub struct MetadataView<'a>(RawMetadataView<'a>);
impl<'a> MetadataView<'a> {
    /// Validates all versioned fields and nested tags before publishing a view.
    /// Accepts Metadata versions 0..13 and an unconsumed suffix, as the Java response wrapper does.
    pub fn parse(version: i16, bytes: &'a [u8]) -> Result<Self, ProtocolError> {
        let (summary, consumed) = walk::<true>(version, bytes, &mut |_| Ok(()))?;
        Ok(Self(RawMetadataView { version, bytes, summary, consumed }))
    }
    /// Returns the exact body, including unknown flexible tags and any unconsumed suffix.
    pub fn raw(self) -> &'a [u8] { self.0.raw() }
    /// Returns bytes after the complete versioned fields and root tags, without interpreting them.
    pub fn trailing_bytes(self) -> &'a [u8] { self.0.trailing_bytes() }
    /// Returns the negotiated protocol version.
    pub fn version(self) -> i16 { self.0.version() }
    /// Reports Java's v9 reliability threshold independently of epoch-field presence.
    pub fn has_reliable_leader_epochs(self) -> bool { self.0.has_reliable_leader_epochs() }
    /// Returns validated counts and the top-level error without constructing collections.
    pub fn summary(self) -> Summary { self.0.summary() }
    /// Calls the visitor synchronously on Rust with borrowed source-order fields.
    /// The immutable body has already passed the identical complete parser; visitor allocations remain caller-owned.
    pub fn visit(self, mut visitor: impl FnMut(Event<'a>)) -> Result<(), ProtocolError> {
        walk::<true>(self.0.version, self.0.bytes, &mut |event| { visitor(event.utf8()?); Ok(()) }).map(|_| ())
    }
}

/// Borrowed broker fields with the original string bytes, including malformed UTF8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawBroker<'a> {
    /// Signed broker node ID.
    pub id: i32,
    /// Original hostname bytes, bounded by the wire string limit.
    pub host: &'a [u8],
    /// Signed advertised port.
    pub port: i32,
    /// Absent before v1 or for a negative nullable length.
    pub rack: Option<&'a [u8]>,
}

/// Borrowed topic fields with the original nullable name bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawTopic<'a> {
    /// Signed Kafka topic error, including unknown codes.
    pub error: i16,
    /// Original topic-name bytes; absent names are accepted from v12.
    pub name: Option<&'a [u8]>,
    /// Network-order UUID bytes; absent before v10.
    pub id: Option<&'a [u8; 16]>,
    /// The wire flag, false before v1.
    pub internal: bool,
    /// Number of following partition events for this topic.
    pub partitions: usize,
}

/// Events preserve order, duplicates and raw string bytes without replacement or normalization.
/// Each Topic precedes its declared Partition events and one TopicEnd.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RawEvent<'a> {
    /// Throttle milliseconds, zero before v3.
    Throttle(i32),
    /// One advertised broker with its original host and rack bytes.
    Broker(RawBroker<'a>),
    /// Cluster/controller fields after all brokers.
    Cluster {
        /// Original nullable cluster ID bytes; absent before v2.
        id: Option<&'a [u8]>,
        /// Signed controller ID; -1 before v1 or when absent.
        controller: i32,
    },
    /// Begins a topic group without decoding its nullable name bytes.
    Topic(RawTopic<'a>),
    /// One partition of the current topic.
    Partition(Partition<'a>),
    /// Fields following the current topic's partitions.
    TopicEnd {
        /// Raw authorization bits; absent before v8.
        authorized_operations: Option<i32>,
    },
    /// Fields after all topics.
    End {
        /// Raw cluster authorization bits from v8..10.
        authorized_operations: Option<i32>,
        /// Raw signed top-level error; zero before v13.
        error: i16,
    },
}
impl<'a> RawEvent<'a> {
    fn utf8(self) -> Result<Event<'a>, ProtocolError> {
        fn text(bytes: &[u8]) -> Result<&str, ProtocolError> { std::str::from_utf8(bytes).map_err(|_| ProtocolError::UnsupportedUtf8) }
        Ok(match self {
            Self::Throttle(value) => Event::Throttle(value),
            Self::Broker(b) => Event::Broker(Broker { id: b.id, host: text(b.host)?, port: b.port, rack: b.rack.map(text).transpose()? }),
            Self::Cluster {id, controller} => Event::Cluster { id: id.map(text).transpose()?, controller },
            Self::Topic(t) => Event::Topic(Topic { error: t.error, name: t.name.map(text).transpose()?, id: t.id, internal: t.internal, partitions: t.partitions }),
            Self::Partition(value) => Event::Partition(value),
            Self::TopicEnd {authorized_operations} => Event::TopicEnd {authorized_operations},
            Self::End {authorized_operations, error} => Event::End {authorized_operations, error},
        })
    }
}

/// A structurally validated Metadata body whose strings remain borrowed byte slices.
/// Construction and visitation allocate no output storage; the caller owns replacement decoding and its admission.
#[derive(Clone, Copy, Debug)]
pub struct RawMetadataView<'a> { version: i16, bytes: &'a [u8], summary: Summary, consumed: usize }
impl<'a> RawMetadataView<'a> {
    /// Validates v0..13 fields and nested tags before exposing a view.
    /// String lengths are checked against the original wire bytes; malformed UTF8 and unconsumed suffixes are retained.
    pub fn parse(version: i16, bytes: &'a [u8]) -> Result<Self, ProtocolError> {
        let (summary, consumed) = walk::<false>(version, bytes, &mut |_| Ok(()))?;
        Ok(Self {version, bytes, summary, consumed})
    }
    /// Returns the complete original body, including unknown tags and any unconsumed suffix.
    pub fn raw(self) -> &'a [u8] { self.bytes }
    /// Returns bytes after all versioned fields and root tags, without interpreting them.
    pub fn trailing_bytes(self) -> &'a [u8] { &self.bytes[self.consumed..] }
    /// Returns the negotiated Metadata version.
    pub fn version(self) -> i16 { self.version }
    /// Reports Java's v9 reliability threshold independently of epoch-field presence.
    pub fn has_reliable_leader_epochs(self) -> bool { self.version >= 9 }
    /// Returns validated counts and original string byte lengths for caller admission.
    pub fn summary(self) -> Summary { self.summary }
    /// Visits validated fields synchronously in original order, with no output allocation.
    /// String replacement and visitor allocations remain caller-owned.
    pub fn visit(self, mut visitor: impl FnMut(RawEvent<'a>)) -> Result<(), ProtocolError> {
        walk::<false>(self.version, self.bytes, &mut |event| { visitor(event); Ok(()) }).map(|_| ())
    }
}

struct Cursor<'a, const VALIDATE_UTF8: bool> { bytes: &'a [u8], at: usize, flexible: bool, strings: usize }
impl<'a, const VALIDATE_UTF8: bool> Cursor<'a, VALIDATE_UTF8> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ProtocolError> {
        let end = self.at.checked_add(len).ok_or(ProtocolError::InvalidLength)?;
        let bytes = self.bytes.get(self.at..end).ok_or(ProtocolError::Truncated)?;
        self.at = end;
        Ok(bytes)
    }
    fn byte(&mut self) -> Result<u8, ProtocolError> { Ok(self.take(1)?[0]) }
    fn i16(&mut self) -> Result<i16, ProtocolError> { let b = self.take(2)?; Ok(i16::from_be_bytes([b[0], b[1]])) }
    fn i32(&mut self) -> Result<i32, ProtocolError> { let b = self.take(4)?; Ok(i32::from_be_bytes([b[0], b[1], b[2], b[3]])) }
    fn varint(&mut self) -> Result<i32, ProtocolError> {
        let mut value = 0u32;
        for shift in (0..35).step_by(7) {
            let byte = self.byte()?;
            value |= u32::from(byte & 127) << shift;
            if byte & 128 == 0 { return Ok(value as i32); }
        }
        Err(ProtocolError::InvalidVarint)
    }
    fn array(&mut self) -> Result<usize, ProtocolError> {
        let n = if self.flexible { self.varint()?.wrapping_sub(1) } else { self.i32()? };
        let n = usize::try_from(n).map_err(|_| ProtocolError::InvalidLength)?;
        if n > self.bytes.len() - self.at { return Err(ProtocolError::Truncated); }
        if n > 1_000_000 { return Err(ProtocolError::ArrayTooLong); }
        Ok(n)
    }
    fn string(&mut self, nullable: bool) -> Result<Option<&'a [u8]>, ProtocolError> {
        let length = if self.flexible { self.varint()?.wrapping_sub(1) } else { i32::from(self.i16()?) };
        if length < 0 { return if nullable { Ok(None) } else { Err(ProtocolError::InvalidLength) }; }
        if length > 32767 { return Err(ProtocolError::StringTooLong); }
        let length = length as usize;
        let value = self.take(length)?;
        if VALIDATE_UTF8 { std::str::from_utf8(value).map_err(|_| ProtocolError::UnsupportedUtf8)?; }
        self.strings = self.strings.checked_add(length).ok_or(ProtocolError::InvalidLength)?;
        Ok(Some(value))
    }
    fn ints(&mut self) -> Result<Int32Array<'a>, ProtocolError> {
        let n = self.array()?;
        Ok(Int32Array(self.take(n.checked_mul(4).ok_or(ProtocolError::InvalidLength)?)?))
    }
    fn tags(&mut self) -> Result<(), ProtocolError> {
        if !self.flexible { return Ok(()); }
        let count = usize::try_from(self.varint()?).map_err(|_| ProtocolError::InvalidLength)?;
        if count > self.bytes.len() - self.at { return Err(ProtocolError::Truncated); }
        if count > 10_000 { return Err(ProtocolError::TooManyTags); }
        for _ in 0..count {
            self.varint()?;
            let length = usize::try_from(self.varint()?).map_err(|_| ProtocolError::InvalidLength)?;
            self.take(length)?;
        }
        Ok(())
    }
}

fn walk<'a, const VALIDATE_UTF8: bool>(version: i16, bytes: &'a [u8], visitor: &mut impl FnMut(RawEvent<'a>) -> Result<(), ProtocolError>) -> Result<(Summary, usize), ProtocolError> {
    if !(0..=13).contains(&version) { return Err(ProtocolError::UnsupportedVersion); }
    let mut c = Cursor::<VALIDATE_UTF8> { bytes, at: 0, flexible: version >= 9, strings: 0 };
    let mut summary = Summary::default();
    summary.throttle_ms = if version >= 3 { c.i32()? } else { 0 };
    visitor(RawEvent::Throttle(summary.throttle_ms))?;
    summary.brokers = c.array()?;
    for _ in 0..summary.brokers {
        let id = c.i32()?;
        let host = c.string(false)?.ok_or(ProtocolError::InvalidLength)?;
        let port = c.i32()?;
        let rack = if version >= 1 { c.string(true)? } else { None };
        c.tags()?;
        visitor(RawEvent::Broker(RawBroker { id, host, port, rack }))?;
    }
    let id = if version >= 2 { c.string(true)? } else { None };
    let controller = if version >= 1 { c.i32()? } else { -1 };
    visitor(RawEvent::Cluster { id, controller })?;
    summary.topics = c.array()?;
    for _ in 0..summary.topics {
        let error = c.i16()?;
        let name = c.string(version >= 12)?;
        let id = if version >= 10 { Some(<&[u8; 16]>::try_from(c.take(16)?).map_err(|_| ProtocolError::Truncated)?) } else { None };
        let internal = version >= 1 && c.byte()? != 0;
        let partitions = c.array()?;
        summary.partitions = summary.partitions.checked_add(partitions).ok_or(ProtocolError::InvalidLength)?;
        visitor(RawEvent::Topic(RawTopic { error, name, id, internal, partitions }))?;
        for _ in 0..partitions {
            let error = c.i16()?;
            let id = c.i32()?;
            let leader = c.i32()?;
            let leader_epoch = if version >= 7 { Some(c.i32()?) } else { None };
            let replicas = c.ints()?;
            let isr = c.ints()?;
            let offline = if version >= 5 { c.ints()? } else { Int32Array(&[]) };
            c.tags()?;
            visitor(RawEvent::Partition(Partition { error, id, leader, leader_epoch, replicas, isr, offline }))?;
        }
        let authorized_operations = if version >= 8 { Some(c.i32()?) } else { None };
        c.tags()?;
        visitor(RawEvent::TopicEnd { authorized_operations })?;
    }
    let authorized_operations = if (8..=10).contains(&version) { Some(c.i32()?) } else { None };
    summary.error = if version >= 13 { c.i16()? } else { 0 };
    c.tags()?;
    summary.string_bytes = c.strings;
    visitor(RawEvent::End { authorized_operations, error: summary.error })?;
    Ok((summary, c.at))
}
