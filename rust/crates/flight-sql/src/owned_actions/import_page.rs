use arrow_array::builder::{ListBuilder, StringBuilder};

use super::*;

pub(crate) const DECIDE_HOSTED_IMPORT_PAGE_V1: &str = "rtdb.owned.DecideHostedImportPage.v1";
const MAX_PAGE_ENTRIES: usize = 128;

pub(super) struct ImportPage<'a> {
    relation: OwnedRelation,
    arrow: &'a [u8],
    placements: Vec<(usize, usize)>,
    _framing: Arc<MemoryReservation>,
}

impl<'a> ImportPage<'a> {
    pub(super) fn split(call: &Call, body: &'a [u8]) -> Result<(Self, &'a [u8]), Status> {
        let (count, body) = body.split_first_chunk::<2>().ok_or_else(|| invalid("Missing import run count"))?;
        let count = usize::from(u16::from_be_bytes(*count));
        if !(2..=MAX_PAGE_ENTRIES).contains(&count) {
            return Err(invalid("Invalid import run count"));
        }
        let framing = call.reserve(count * 32 + 512)?;
        let (schedule, body) = body.split_at_checked(count * 4).ok_or_else(|| invalid("Truncated import run schedule"))?;
        let placements = schedule.as_chunks::<4>().0.iter().map(|[hi, lo, rows_hi, rows_lo]| {
            (usize::from(u16::from_be_bytes([*hi, *lo])), usize::from(u16::from_be_bytes([*rows_hi, *rows_lo])))
        }).collect::<Vec<_>>();
        let (relation, body) = split_relation(body)?;
        if !relation.is_root_data() {
            return Err(invalid("An import page requires a Root relation"));
        }
        let (length, body) = body.split_first_chunk::<4>().ok_or_else(|| invalid("Missing import batch length"))?;
        let (arrow, body) = body.split_at_checked(u32::from_be_bytes(*length) as usize)
            .ok_or_else(|| invalid("Truncated import batch"))?;
        let inline = ordered_group_count(body)?;
        if inline + count > MAX_GROUPED_COMMIT_ROWS || placements.last().is_none_or(|(before, _)| *before > inline)
            || placements.windows(2).any(|pair| pair[0].0 >= pair[1].0)
            || placements.iter().any(|&(_, rows)| rows == 0)
            || placements.iter().map(|&(_, rows)| rows).sum::<usize>() > MAX_PAGE_ENTRIES
        {
            return Err(invalid("Invalid import run schedule"));
        }
        Ok((Self { relation, arrow, placements, _framing: framing }, body))
    }

    pub(super) fn relation(&self) -> &OwnedRelation { &self.relation }

    pub(super) fn decode(
        &self, service: &Service, call: &Call, envelope: &Bytes, inline: Vec<(OwnedRelation, &[u8])>,
        writes: &mut Vec<TxOp>, rows_left: &mut usize, effects: CommitEffects,
    ) -> Result<Vec<(Arc<MemoryReservation>, Arc<MemoryReservation>)>, Status> {
        let page = require_identity(decode_retained_batch(call, envelope.slice_ref(self.arrow))?)?;
        let batch = page.batch();
        if batch.num_rows() != self.placements.iter().map(|&(_, rows)| rows).sum::<usize>() {
            return Err(invalid("The import schedule does not consume its batch"));
        }
        let ops = batch.column_by_name("_op").and_then(|column| column.as_any().downcast_ref::<StringArray>())
            .ok_or_else(|| invalid("The import page needs create and check operations"))?;
        let mut creates = 0;
        for operation in ops.iter() {
            match operation {
                Some("create") => creates += 1,
                Some("check") => (),
                _ => return Err(invalid("The import page needs create and check operations")),
            }
        }
        if !(1..=64).contains(&creates) {
            return Err(invalid("The import page requires 1 to 64 creates"));
        }
        let mut retained = Vec::with_capacity(inline.len() + self.placements.len());
        let mut placements = self.placements.iter().copied().peekable();
        let mut offset = 0;
        let inline_count = inline.len();
        let mut inline = inline.into_iter();
        for index in 0..=inline_count {
            if let Some((_, rows)) = placements.next_if(|(before, _)| *before == index) {
                let run = import_run(&page, offset, rows, call.memory())?;
                retained.push(service.append_decoded_owned_group(call, &self.relation, &run, writes, rows_left, effects)?);
                offset += rows;
            }
            if let Some((relation, arrow)) = inline.next() {
                let batch = require_identity(decode_retained_batch(call, envelope.slice_ref(arrow))?)?;
                retained.push(service.append_decoded_owned_group(call, &relation, &batch, writes, rows_left, effects)?);
            }
        }
        Ok(retained)
    }
}

fn import_run(page: &OwnedBatch, offset: usize, rows: usize, memory: &Arc<MemoryBudget>) -> Result<OwnedBatch, Status> {
    let batch = page.batch().slice(offset, rows);
    let schema = batch.schema_ref();
    let _scratch = memory.reserve(wire::add(wire::schema_bound(schema)?, wire::mul(
        wire::mul(rows, batch.num_columns())?, 64,
    )?)?).map_err(status)?;
    let absence = batch.column_by_name("_absent").map(|column| {
        column.as_any().downcast_ref::<ListArray>().ok_or_else(|| invalid("Invalid import field presence"))
    }).transpose()?;
    let omitted = (0..batch.num_rows()).map(|row| {
        let Some(column) = absence else { return Ok(BTreeSet::new()); };
        let Some(list) = (ArrowCell { field: schema.field_with_name("_absent").map_err(arrow)?, array: column, row }).list()
        else { return Err(invalid("Invalid import field presence")); };
        let mut omitted = BTreeSet::new();
        for value in list {
            let name = value.utf8().ok_or_else(|| invalid("Invalid import field presence"))?;
            let value = ArrowCell::named(&batch, name, row).ok_or_else(|| invalid("Invalid import field presence"))?;
            if name.starts_with('_') || !value.is_null().map_err(status)? || !omitted.insert(name) {
                return Err(invalid("Invalid import field presence"));
            }
        }
        Ok(omitted)
    }).collect::<Result<Vec<_>, Status>>()?;
    let payload = schema.fields().iter().filter(|field| !field.name().starts_with('_')
        && omitted.iter().any(|row| !row.contains(field.name().as_str())))
        .map(|field| field.name().as_str()).collect::<BTreeSet<_>>();
    let mut columns: Vec<(String, ArrayRef)> = Vec::with_capacity(batch.num_columns());
    let mut absence_index = None;
    for (field, array) in schema.fields().iter().zip(batch.columns()) {
        let name = field.name();
        if name == "_absent" {
            if !payload.is_empty() {
                absence_index = Some(columns.len());
            }
        } else if payload.contains(name.as_str()) || (name.starts_with('_')
            && !(matches!(name.as_str(), "_valid_from_offset_micros" | "_valid_to_offset_micros" | "_ordered_micros")
                && array.null_count() == batch.num_rows()))
        {
            columns.push((name.clone(), Arc::clone(array)));
        }
    }
    // The original run determines its field presence and Arrow field nullability.
    let batch = RecordBatch::try_from_iter(columns).map_err(arrow)?;
    let run = OwnedBatch::with_reservation(batch, Arc::clone(page.reservation())).map_err(status)?;
    let Some(index) = absence_index else { return Ok(run); };
    let (names, bytes) = omitted.iter().flat_map(|row| row.intersection(&payload))
        .try_fold((0, 0), |(names, bytes), name| Ok::<_, Status>((names + 1, wire::add(bytes, name.len())?)))?;
    let charge = memory.reserve(wire::add(4096, wire::add(wire::mul(names, 32)?, wire::mul(bytes, 2)?)?)?).map_err(status)?;
    let mut absent = ListBuilder::with_capacity(StringBuilder::with_capacity(names, bytes), rows);
    for row in &omitted {
        for name in row.intersection(&payload) {
            absent.values().append_value(name);
        }
        absent.append(true);
    }
    let absent = Arc::new(absent.finish());
    let field = Field::new("_absent", absent.data_type().clone(), false);
    run.with_inserted_column(index, field, absent, charge).map_err(status)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_import_page_restores_original_run_schemas() {
        let mut absent = ListBuilder::new(StringBuilder::new());
        absent.values().append_value("second");
        absent.append(true);
        absent.values().append_value("first");
        absent.append(true);
        absent.append(true);
        let source = RecordBatch::try_from_iter([
            ("_id", Arc::new(StringArray::from(vec!["a", "b", "c"])) as ArrayRef),
            ("_op", Arc::new(StringArray::from(vec!["create"; 3]))),
            ("_absent", Arc::new(absent.finish())),
            ("first", Arc::new(BinaryArray::from(vec![Some(b"a".as_slice()), None, None]))),
            ("second", Arc::new(BinaryArray::from(vec![None, Some(b"b".as_slice()), Some(b"c".as_slice())]))),
        ]).unwrap();
        let memory = MemoryBudget::new(8 * 1024 * 1024);
        let page = OwnedBatch::new(source, &memory).unwrap();
        let first = import_run(&page, 0, 1, &memory).unwrap();
        let schema = first.batch().schema_ref();
        assert!(!schema.field_with_name("first").unwrap().is_nullable());
        assert!(schema.field_with_name("second").is_err());
        assert_eq!(owned_payload_columns(first.batch(), 0, &["_op"]).unwrap(), [0, 3]);
        let rest = import_run(&page, 1, 2, &memory).unwrap();
        let schema = rest.batch().schema_ref();
        assert!(schema.field_with_name("first").unwrap().is_nullable());
        assert!(!schema.field_with_name("second").unwrap().is_nullable());
        assert_eq!(owned_payload_columns(rest.batch(), 0, &["_op"]).unwrap(), [0, 4]);
        assert_eq!(owned_payload_columns(rest.batch(), 1, &["_op"]).unwrap(), [0, 3, 4]);
    }
}
