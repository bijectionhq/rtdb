use super::*;

pub(in crate::connect::avro) trait BuildDatum {
    type Node;

    fn accepts_packed_arrays(&self) -> bool { false }

    fn scalar(&mut self, value: Avro) -> Self::Node;
    fn record(&mut self, fields: Vec<(String, Self::Node)>) -> Self::Node;
    fn array(&mut self, values: Vec<Self::Node>) -> Self::Node;
    fn map(
        &mut self,
        first_block: usize,
        entries: Vec<(MapKey, Self::Node)>,
        strings: bool,
    ) -> Self::Node;
    fn union(&mut self, index: u32, child: Self::Node) -> Self::Node;
    fn reader_default(&mut self, value: &Avro) -> Self::Node;
}

pub(in crate::connect::avro) struct PhysicalBuilder;

impl BuildDatum for PhysicalBuilder {
    type Node = Avro;

    fn accepts_packed_arrays(&self) -> bool { true }

    fn scalar(&mut self, value: Avro) -> Avro {
        value
    }
    fn record(&mut self, fields: Vec<(String, Avro)>) -> Avro {
        Avro::Record(fields)
    }
    fn array(&mut self, values: Vec<Avro>) -> Avro {
        Avro::Array(values)
    }
    fn map(&mut self, first_block: usize, entries: Vec<(MapKey, Avro)>, strings: bool) -> Avro {
        Avro::Map(AvroMap::new(first_block, entries, strings))
    }
    fn union(&mut self, index: u32, child: Avro) -> Avro {
        Avro::Union(index, Box::new(child))
    }
    fn reader_default(&mut self, value: &Avro) -> Avro {
        value.clone()
    }
}
