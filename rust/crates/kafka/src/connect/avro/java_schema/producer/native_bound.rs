use super::*;

#[derive(Clone, Copy)]
pub(super) struct NativeFootprint {
    document_bytes: usize,
    json_items: usize,
    schema_items: usize,
    full_name_units: usize,
    depth: usize,
}
const COLLECTION_ENVELOPE: usize = 16;
const ORDINARY_LIVE_COPIES: usize = 16;
const DOCUMENT_TEXT_ENVELOPE: usize = 8;
const FULL_NAME_UTF8_ENVELOPE: usize = 4;
const PER_ITEM_BOOKKEEPING: usize = 256;
const OWNER_BOOKKEEPING: usize = 65_536;

impl NativeFootprint {
    pub(super) fn new() -> Self {
        Self { document_bytes: 0, json_items: 0, schema_items: 0, full_name_units: 0, depth: 0 }
    }
    pub(super) fn document(&mut self, bytes: usize, root: &JsonNode) -> Result<()> {
        self.document_bytes = add(self.document_bytes, bytes)?;
        self.json_items = add(self.json_items, items(root, 0)?)?;
        Ok(())
    }
    pub(super) fn schema(&mut self, depth: usize) -> Result<()> {
        self.schema_items = add(self.schema_items, 1)?;
        self.depth = self.depth.max(depth);
        Ok(())
    }
    pub(super) fn name(&mut self, name: &Name) -> Result<()> {
        self.name_occurrences(name, 1)
    }
    pub(super) fn name_occurrences(&mut self, name: &Name, occurrences: usize) -> Result<()> {
        let units = name.full().map_or(0, |name| name.view().len_units());
        self.full_name_units = add(self.full_name_units, mul(units, occurrences)?)?;
        Ok(())
    }
    pub(super) fn bound(self) -> Result<usize> {
        let item = add(
            add(size_of::<serde_json::Value>(), size_of::<apache_avro::Schema>())?,
            add(size_of::<apache_avro::schema::RecordField>(), PER_ITEM_BOOKKEEPING)?,
        )?;
        let controls = mul(add(add(self.json_items, self.schema_items)?, 1)?, item)?;
        let payload = add(
            mul(self.document_bytes, DOCUMENT_TEXT_ENVELOPE)?,
            mul(self.full_name_units, FULL_NAME_UTF8_ENVELOPE)?,
        )?;
        let copies = add(mul(add(self.depth, 1)?, 2)?, ORDINARY_LIVE_COPIES)?;
        add(mul(mul(add(controls, payload)?, copies)?, COLLECTION_ENVELOPE)?, OWNER_BOOKKEEPING)
    }
}
fn items(node: &JsonNode, depth: usize) -> Result<usize> {
    guard_depth(depth, "Native parse measurement exceeds the native traversal depth limit of 128")?;
    let children = match node.value() {
        JsonValue::Array(values) => values.iter().try_fold(0usize, |count, node| add(count, items(node, depth + 1)?))?,
        JsonValue::Object(values) => values.iter().try_fold(values.len(), |count, (_, node)| add(count, items(node, depth + 1)?))?,
        _ => 0,
    };
    add(children, 1)
}

#[cfg(test)]
mod tests;
