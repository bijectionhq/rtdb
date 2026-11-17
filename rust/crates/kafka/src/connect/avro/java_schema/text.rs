use super::*;
use rtdb_types::java_text::{JavaString, JavaText};

#[derive(Clone, Debug)]
pub(in crate::connect::avro) struct Text { value: JavaString, charge: Arc<MemoryReservation> }
impl Text {
    pub(in crate::connect::avro) fn copy_units(source: &[u16], context: &mut ConversionContext) -> Result<Self> {
        let mut charge = None;
        let value = JavaString::copy_utf16_with_admission(source, |bytes| {
            let lease = context.reserve(bytes)?;
            charge = Some(Arc::clone(&lease));
            Ok(lease)
        })?;
        Ok(Self { value, charge: charge.unwrap() })
    }
    pub(in crate::connect::avro) fn copy(source: JavaText<'_>, context: &mut ConversionContext) -> Result<Self> {
        let count = source.len_units();
        if count > i32::MAX as usize { return Err(invalid("Java String exceeds its signed32 length domain")); }
        context.reserve(add(mul(count, size_of::<u16>())?, 256)?)?;
        let mut units = Vec::new();
        units.try_reserve_exact(count).map_err(allocation)?;
        units.extend(source.units());
        Self::copy_units(&units, context)
    }
    pub(in crate::connect::avro) fn view(&self) -> JavaText<'_> { self.value.view() }
    pub(in crate::connect::avro) fn retain(&self, context: &mut ConversionContext) -> Result<()> { context.retain(Arc::clone(&self.charge)) }
    #[cfg(test)]
    pub(super) fn same_storage(&self, other: &Self) -> bool { Arc::ptr_eq(&self.charge, &other.charge) }
}
impl PartialEq for Text { fn eq(&self, other: &Self) -> bool { self.value == other.value } }
impl Eq for Text {}
impl PartialOrd for Text { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for Text { fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.view().units().cmp(other.view().units()) } }
