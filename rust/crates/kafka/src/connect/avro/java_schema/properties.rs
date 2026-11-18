use super::*;

pub(in crate::connect::avro) struct Properties {
    members: Members,
    order: Vec<usize>,
    charges: Vec<Arc<MemoryReservation>>,
}
impl Properties {
    pub(in crate::connect::avro) fn empty() -> Self { Self { members: Members::empty(), order: Vec::new(), charges: Vec::new() } }
    pub(super) fn get(&self, key: &str) -> Option<&JsonNode> { self.members.get_text(JavaText::from_utf8(key)) }
    pub(in crate::connect::avro) fn from_members(members: Members, context: &mut ConversionContext) -> Result<Self> {
        let (order, charge) = super::property_order::snapshot(&members, context)?;
        Ok(Self { members, order, charges: vec![charge] })
    }
    pub(super) fn equal(&self, other: &Self, context: &mut ConversionContext, depth: usize) -> Result<bool> {
        if std::ptr::eq(self, other) { return Ok(true); }
        for index in &self.order {
            let (key, value) = self.members.entry(*index);
            let Some(right) = other.members.get(key) else { return Ok(false); };
            if !right.equal(value, context, depth + 1)? { return Ok(false); }
        }
        for (key, right) in other.members.iter() {
            let Some(left) = self.members.get(key) else { return Ok(false); };
            if !right.equal(left, context, depth + 1)? { return Ok(false); }
        }
        Ok(true)
    }
    pub(super) fn hash(&self, context: &mut ConversionContext, depth: usize) -> Result<i32> {
        let mut hash = 0i32;
        for index in &self.order {
            let (key, value) = self.members.entry(*index);
            hash = hash.wrapping_add(key.view().java_hash_code() ^ value.hash(context, depth + 1)?);
        }
        Ok(hash)
    }
    pub(super) fn retain<'a>(&'a self, context: &mut ConversionContext, seen: &mut json::JsonSeen<'a>) -> Result<()> {
        for charge in &self.charges { context.retain(Arc::clone(charge))?; }
        self.members.retain(context, seen, 0)
    }
}

#[cfg(test)]
mod tests;
