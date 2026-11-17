/// A rejected wire representation; no partial visitor events have been delivered by parse.
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
    /// Java's malformed UTF8 replacement behavior is outside this transport profile.
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
    /// Total borrowed UTF8 payload bytes across all strings.
    pub string_bytes: usize,
    /// Top-level Kafka error code; zero before Metadata v13.
    pub error: i16,
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
    /// Returns whether the array has no elements.
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
    /// One broker entry.
    Broker(Broker<'a>),
    /// Cluster/controller values after all broker entries; -1 means no controller.
    Cluster {
        /// Nullable cluster ID; absent before v2.
        id: Option<&'a str>,
        /// Controller node ID; -1 before v1 or when absent.
        controller: i32,
    },
    /// One topic header.
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
pub struct MetadataView<'a> { version: i16, bytes: &'a [u8], summary: Summary, consumed: usize }
impl<'a> MetadataView<'a> {
    /// Validates all versioned fields and nested tags before publishing a view.
    /// Accepts Metadata versions 0..13 and an unconsumed suffix, as the Java response wrapper does.
    /// Errors never call a visitor.
    pub fn parse(version: i16, bytes: &'a [u8]) -> Result<Self, ProtocolError> {
        let (summary, consumed) = walk(version, bytes, &mut |_| {})?;
        Ok(Self { version, bytes, summary, consumed })
    }
    /// Returns the exact body, including unknown flexible tags and any unconsumed suffix.
    pub fn raw(self) -> &'a [u8] { self.bytes }
    /// Returns bytes after the complete versioned fields and root tags, without interpreting them.
    pub fn trailing_bytes(self) -> &'a [u8] { &self.bytes[self.consumed..] }
    /// Returns the negotiated protocol version.
    pub fn version(self) -> i16 { self.version }
    /// Matches Java MetadataResponse's reliability threshold, independently of epoch presence.
    pub fn has_reliable_leader_epochs(self) -> bool { self.version >= 9 }
    /// Returns validated counts and the top-level error without constructing collections.
    pub fn summary(self) -> Summary { self.summary }
    /// Calls the visitor synchronously on Rust with borrowed source-order fields.
    /// The immutable body has already passed the identical complete parser; visitor allocations remain caller-owned.
    pub fn visit(self, mut visitor: impl FnMut(Event<'a>)) -> Result<(), ProtocolError> {
        walk(self.version, self.bytes, &mut visitor).map(|_| ())
    }
}

struct Cursor<'a> { bytes: &'a [u8], at: usize, flexible: bool, strings: usize }
impl<'a> Cursor<'a> {
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
    fn string(&mut self, nullable: bool) -> Result<Option<&'a str>, ProtocolError> {
        let length = if self.flexible { self.varint()?.wrapping_sub(1) } else { i32::from(self.i16()?) };
        if length < 0 { return if nullable { Ok(None) } else { Err(ProtocolError::InvalidLength) }; }
        if length > 32767 { return Err(ProtocolError::StringTooLong); }
        let length = length as usize;
        let value = std::str::from_utf8(self.take(length)?).map_err(|_| ProtocolError::UnsupportedUtf8)?;
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

fn walk<'a>(version: i16, bytes: &'a [u8], visitor: &mut impl FnMut(Event<'a>)) -> Result<(Summary, usize), ProtocolError> {
    if !(0..=13).contains(&version) { return Err(ProtocolError::UnsupportedVersion); }
    let mut c = Cursor { bytes, at: 0, flexible: version >= 9, strings: 0 };
    let mut summary = Summary::default();
    visitor(Event::Throttle(if version >= 3 { c.i32()? } else { 0 }));
    summary.brokers = c.array()?;
    for _ in 0..summary.brokers {
        let id = c.i32()?;
        let host = c.string(false)?.ok_or(ProtocolError::InvalidLength)?;
        let port = c.i32()?;
        let rack = if version >= 1 { c.string(true)? } else { None };
        c.tags()?;
        visitor(Event::Broker(Broker { id, host, port, rack }));
    }
    let id = if version >= 2 { c.string(true)? } else { None };
    let controller = if version >= 1 { c.i32()? } else { -1 };
    visitor(Event::Cluster { id, controller });
    summary.topics = c.array()?;
    for _ in 0..summary.topics {
        let error = c.i16()?;
        let name = c.string(version >= 12)?;
        let id = if version >= 10 { Some(<&[u8; 16]>::try_from(c.take(16)?).map_err(|_| ProtocolError::Truncated)?) } else { None };
        let internal = version >= 1 && c.byte()? != 0;
        let partitions = c.array()?;
        summary.partitions = summary.partitions.checked_add(partitions).ok_or(ProtocolError::InvalidLength)?;
        visitor(Event::Topic(Topic { error, name, id, internal, partitions }));
        for _ in 0..partitions {
            let error = c.i16()?;
            let id = c.i32()?;
            let leader = c.i32()?;
            let leader_epoch = if version >= 7 { Some(c.i32()?) } else { None };
            let replicas = c.ints()?;
            let isr = c.ints()?;
            let offline = if version >= 5 { c.ints()? } else { Int32Array(&[]) };
            c.tags()?;
            visitor(Event::Partition(Partition { error, id, leader, leader_epoch, replicas, isr, offline }));
        }
        let authorized_operations = if version >= 8 { Some(c.i32()?) } else { None };
        c.tags()?;
        visitor(Event::TopicEnd { authorized_operations });
    }
    let authorized_operations = if (8..=10).contains(&version) { Some(c.i32()?) } else { None };
    summary.error = if version >= 13 { c.i16()? } else { 0 };
    c.tags()?;
    summary.string_bytes = c.strings;
    visitor(Event::End { authorized_operations, error: summary.error });
    Ok((summary, c.at))
}
