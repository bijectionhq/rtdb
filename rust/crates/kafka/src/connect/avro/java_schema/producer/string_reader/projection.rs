use super::*;
use rtdb_types::jvm_number::Number;

trait Sink { fn text(&mut self, text: &str) -> Result<()>; }
struct Measure(usize);
impl Sink for Measure { fn text(&mut self, text: &str) -> Result<()> { self.0 = add(self.0, text.len())?; Ok(()) } }
struct Output<'a> { text: &'a mut String, limit: usize }
impl Sink for Output<'_> {
    fn text(&mut self, text: &str) -> Result<()> {
        if text.len() > self.limit - self.text.len() { return Err(unrepresentable()); }
        self.text.push_str(text);
        Ok(())
    }
}
fn unrepresentable() -> Anomaly {
    Anomaly::unsupported("rtdb/avro-reader-native-projection", "Source schema tree cannot be represented by the native backend JSON profile")
}
pub(super) fn render(tree: &JsonNode, context: &mut ConversionContext) -> Result<java_text::Projection> {
    let mut measure = Measure(0);
    value(tree, &mut measure, context, 0)?;
    let charge = context.reserve(add(mul(measure.0, 16)?, 4096)?)?;
    let mut text = String::new();
    text.try_reserve_exact(measure.0).map_err(allocation)?;
    value(tree, &mut Output { text: &mut text, limit: measure.0 }, context, 0)?;
    if text.len() != measure.0 { return Err(unrepresentable()); }
    Ok(java_text::Projection { text, charge })
}
fn string(text: JavaText<'_>, sink: &mut impl Sink) -> Result<()> {
    sink.text("\"")?;
    for point in text.code_points() {
        match point {
            34 => sink.text("\\\"")?, 92 => sink.text("\\\\")?,
            0..=31 => {
                let hex = b"0123456789abcdef";
                let bytes = [92, 117, 48, 48, hex[(point >> 4) as usize], hex[(point & 15) as usize]];
                sink.text(std::str::from_utf8(&bytes).unwrap())?;
            }
            point => {
                let scalar = char::from_u32(point).ok_or_else(unrepresentable)?;
                let mut bytes = [0u8; 4];
                sink.text(scalar.encode_utf8(&mut bytes))?;
            }
        }
    }
    sink.text("\"")
}
fn numeric(number: Number, sink: &mut impl Sink, context: &mut ConversionContext) -> Result<()> {
    context.reserve(add(mul(number.text_capacity().map_err(|_| unrepresentable())?, 16)?, 4096)?)?;
    sink.text(&number.text())
}
fn value(node: &JsonNode, sink: &mut impl Sink, context: &mut ConversionContext, depth: usize) -> Result<()> {
    guard_depth(depth, "Backend projection exceeds the native traversal depth limit of128")?;
    match node.value() {
        JsonValue::Null => sink.text("null"),
        JsonValue::Bool(value) => sink.text(if *value { "true" } else { "false" }),
        JsonValue::I32(value) => numeric(Number::Int(*value), sink, context),
        JsonValue::I64(value) => numeric(Number::Long(*value), sink, context),
        JsonValue::Integer(value) => value.with_decimal(context, |text| sink.text(text)),
        JsonValue::F64(value) if value.is_finite() => numeric(Number::Double(*value), sink, context),
        JsonValue::Text(text) => string(text.view(), sink),
        JsonValue::Array(values) => {
            sink.text("[")?;
            for (index, child) in values.iter().enumerate() {
                if index != 0 { sink.text(",")?; }
                value(child, sink, context, depth + 1)?;
            }
            sink.text("]")
        }
        JsonValue::Object(fields) => {
            sink.text("{")?;
            for (index, (key, child)) in fields.iter().enumerate() {
                if index != 0 { sink.text(",")?; }
                string(key.view(), sink)?;
                sink.text(":")?;
                value(child, sink, context, depth + 1)?;
            }
            sink.text("}")
        }
        _ => Err(unrepresentable()),
    }
}
