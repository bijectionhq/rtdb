use std::collections::BTreeMap;

use base64::{alphabet, engine::{DecodePaddingMode, GeneralPurpose, GeneralPurposeConfig}};
use prost::Message;

pub(crate) const XTDB_BASE64: GeneralPurpose = GeneralPurpose::new(&alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent).with_decode_allow_trailing_bits(true));

// Session action messages follow https://github.com/apache/arrow/blob/main/format/Flight.proto.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct SessionOption { #[prost(oneof = "OptionValue", tags = "1, 2, 3, 4, 5")] pub value: Option<OptionValue> }
#[derive(Clone, PartialEq, prost::Oneof)]
pub(crate) enum OptionValue { #[prost(string, tag = "1")] String(String), #[prost(bool, tag = "2")] Bool(bool), #[prost(sfixed64, tag = "3")] Integer(i64), #[prost(double, tag = "4")] Double(f64), #[prost(message, tag = "5")] Strings(StringList) }
#[derive(Clone, PartialEq, Message)]
pub(crate) struct StringList { #[prost(string, repeated, tag = "1")] pub values: Vec<String> }
#[derive(Clone, PartialEq, Message)]
pub(crate) struct SessionOptions { #[prost(btree_map = "string, message", tag = "1")] pub session_options: BTreeMap<String, SessionOption> }
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Empty {}
#[derive(Clone, PartialEq, Message)]
pub(crate) struct OptionError { #[prost(int32, tag = "1")] pub value: i32 }
#[derive(Clone, PartialEq, Message)]
pub(crate) struct SetOptionsResult { #[prost(btree_map = "string, message", tag = "1")] pub errors: BTreeMap<String, OptionError> }
#[derive(Clone, PartialEq, Message)]
pub(crate) struct CloseResult {
    #[prost(int32, tag = "1")]
    pub status: i32,
    /// Store calls and Flight-owned reply buffers of this closed session.
    #[prost(bool, tag = "2")]
    pub store_reads_drained: bool,
    /// An exact original Store session and its signed terminal evidence.
    #[prost(bytes, tag = "3")]
    pub store_watch_drain: Vec<u8>,
}
#[derive(Clone, PartialEq, Message)]
pub(crate) struct PreparedResult {
    #[prost(bytes = "bytes", tag = "1")] pub prepared_statement_handle: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "2")] pub dataset_schema: bytes::Bytes,
    #[prost(bytes = "bytes", tag = "3")] pub parameter_schema: bytes::Bytes,
    #[prost(bool, tag = "4")] pub is_update: bool,
}
