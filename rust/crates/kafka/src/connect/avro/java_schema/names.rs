use super::*;
use rtdb_types::java_text::JavaText;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::connect::avro) struct Name { full: Option<Text> }
impl Name {
    pub(in crate::connect::avro) fn new(name: Option<JavaText<'_>>, namespace: Option<JavaText<'_>>, context: &mut ConversionContext) -> Result<Self> {
        let Some(name) = name else { return Ok(Self { full: None }); };
        let dot = name.units().enumerate().filter_map(|(index, unit)| (unit == 46).then_some(index)).last();
        let (namespace, simple) = match dot {
            Some(dot) => (Some(name.slice_units(0..dot).unwrap()), name.slice_units(dot + 1..name.len_units()).unwrap()),
            None => (namespace, name),
        };
        let namespace = namespace.filter(|value| !value.is_empty());
        let mut units = simple.units();
        if !units.next().is_some_and(|unit| super::super::names::unit_flags(unit) & 1 != 0)
            || !units.all(|unit| super::super::names::unit_flags(unit) & 2 != 0) {
            return Err(invalid("Invalid Java Avro simple name"));
        }
        let count = add(simple.len_units(), namespace.map_or(Ok(0), |value| add(value.len_units(), 1))?)?;
        context.reserve(add(mul(count, size_of::<u16>())?, 256)?)?;
        let mut full = Vec::new();
        full.try_reserve_exact(count).map_err(allocation)?;
        if let Some(namespace) = namespace { full.extend(namespace.units()); full.push(46); }
        full.extend(simple.units());
        Ok(Self { full: Some(Text::copy_units(&full, context)?) })
    }
    pub(in crate::connect::avro) fn full(&self) -> Option<&Text> { self.full.as_ref() }
    pub(super) fn retain(&self, context: &mut ConversionContext) -> Result<()> {
        if let Some(full) = &self.full { full.retain(context)?; }
        Ok(())
    }
}
