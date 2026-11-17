use super::*;

impl ExecutionVector {
    /// Returns a retained reader after checking the requested outer positions.
    /// Translated signed child addresses are retained without admitting their final payload access.
    /// Index selection of a named union leg translates its offsets and drops its discriminator mask.
    /// Range selection does the same for a direct leg; a range over an already indirect reader retains that reader's mask.
    /// A direct range retains a Slice; another range over any indirect reader allocates translated selection indices.
    /// `All` is a range covering every row; metadata allocation can fail without changing the input.
    pub fn select(&self, selection: &Selection) -> Result<Self> {
        if let Selection::Indices(indices) = selection { vector_length(indices.len())?; }
        selection.validate(self.len())?;
        let (offset, length) = match selection {
            Selection::All => (0, self.len()),
            Selection::Range { offset, length } => (*offset, *length),
            Selection::Indices(indices) => return self.select_indices(indices.iter().map(|index| *index as usize), indices.len()),
        };
        if matches!((&self.view, &self.address), (ReaderView::UnionLeg { .. }, Address::Identity { .. })) {
            self.select_indices(offset..offset + length, length)
        } else if matches!(&self.address, Address::Identity { .. }) {
            Ok(Self { view: self.view.clone(), address: Address::Range { offset, len: length } })
        } else {
            let reservation = self.budget().reserve(index_bytes(length)?)?;
            let mut rows = Vec::with_capacity(length);
            for index in offset..offset + length { rows.push(self.address.translate(PhysicalIndex::from_position(index)?)?); }
            let address = Address::Indices { selection: Arc::new(Indices { rows, _reservation: reservation }), offset: 0, len: length };
            Ok(Self { view: self.view.clone(), address })
        }
    }

    fn select_indices(&self, indices: impl Iterator<Item = usize>, length: usize) -> Result<Self> {
        vector_length(length)?;
        let reservation = self.budget().reserve(index_bytes(length)?)?;
        let mut rows = Vec::with_capacity(length);
        for index in indices { rows.push(self.selected_row(PhysicalIndex::from_position(index)?)?); }
        let address = Address::Indices { selection: Arc::new(Indices { rows, _reservation: reservation }), offset: 0, len: length };
        Ok(Self { view: ReaderView::Direct(Arc::clone(self.selected_node())), address })
    }

    fn selected_node(&self) -> &Arc<PhysicalNode> {
        match &self.view { ReaderView::Direct(node) => node, ReaderView::UnionLeg { inner, .. } => inner.selected_node() }
    }

    fn selected_row(&self, index: PhysicalIndex) -> Result<PhysicalIndex> {
        let row = self.address.translate(index)?;
        match &self.view {
            ReaderView::Direct(_) => Ok(row),
            ReaderView::UnionLeg { union, inner, .. } => inner.selected_row(union.union_offset_index(row)?),
        }
    }

    /// Implements Struct child and DenseUnion named-leg lookup while retaining the reader's selection.
    /// Missing names return None; other classes, including run-end encoding, reject vectorFor.
    /// A union leg masks is_null only; its getters still delegate through the stored offset.
    pub fn vector_for(&self, name: &str) -> Result<Option<Self>> {
        match &self.view {
            ReaderView::Direct(node) => match &node.storage {
                NodeLayout::Struct { children, .. } => Ok(children.iter().find(|child| child.field.name() == name).map(|child| Self { view: ReaderView::Direct(Arc::clone(child)), address: self.address.clone() })),
                NodeLayout::Union { children, .. } => {
                    let DataType::Union(fields, _) = node.field.data_type() else { return Err(layout()); };
                    let Some((index, (id, _))) = fields.iter().enumerate().find(|(_, (_, field))| field.name() == name) else { return Ok(None); };
                    self.leg(node, id, Self::direct(Arc::clone(&children[index]))).map(Some)
                }
                _ => Err(unsupported("vectorFor")),
            },
            ReaderView::UnionLeg { union, type_id, inner, .. } => inner.vector_for(name)?.map(|child| self.leg(union, *type_id, child)).transpose(),
        }
    }

    fn leg(&self, union: &Arc<PhysicalNode>, type_id: i8, inner: Self) -> Result<Self> {
        let reservation = self.budget().reserve(512)?;
        Ok(Self { view: ReaderView::UnionLeg { union: Arc::clone(union), type_id, inner: Arc::new(inner), _reservation: reservation }, address: self.address.clone() })
    }

    /// Looks up a Struct child, including a Struct reached through a named union leg.
    /// Other classes reject the method; parent nullness does not mask child storage.
    pub fn struct_child(&self, name: &str) -> Result<Option<Self>> {
        if !matches!(self.field().data_type(), DataType::Struct(_)) { return Err(unsupported("vectorFor")); }
        self.vector_for(name)
    }

    /// Returns complete elements from a plain list/map or a named leg around one.
    /// Parent selections do not select elements; direct union and run-end wrappers reject listElements.
    pub fn list_elements(&self) -> Result<Self> {
        match &self.view {
            ReaderView::UnionLeg { inner, .. } => inner.list_elements(),
            ReaderView::Direct(node) => match &node.storage {
                NodeLayout::List { elements, .. } | NodeLayout::FixedList { elements, .. } => Ok(Self::direct(Arc::clone(elements))),
                _ => Err(unsupported("listElements")),
            },
        }
    }

    /// Borrows union leg names in declaration order; named-leg readers delegate to their inner reader.
    /// Non-union readers, including run-end wrappers, return None.
    pub fn leg_names(&self) -> Option<impl Iterator<Item = &str>> {
        let node = self.node();
        let DataType::Union(fields, _) = node.field.data_type() else { return None; };
        Some(fields.iter().map(|(_, field)| field.name().as_str()))
    }

    /// Borrows Struct keys in declaration order; named-leg readers delegate and other classes return None.
    pub fn key_names(&self) -> Option<impl Iterator<Item = &str>> {
        let DataType::Struct(fields) = self.node().field.data_type() else { return None; };
        Some(fields.iter().map(|field| field.name().as_str()))
    }

    /// Returns the complete first Struct child, or the Map entry reader's first child.
    /// Parent selections and union-leg masks do not select these keys; empty Struct and other classes fail.
    pub fn map_keys(&self) -> Result<Self> { self.map_column(false) }

    /// Returns the complete last Struct child, or the Map entry reader's last child, under map_keys admission.
    pub fn map_values(&self) -> Result<Self> { self.map_column(true) }

    fn map_column(&self, last: bool) -> Result<Self> {
        match &self.view {
            ReaderView::UnionLeg { inner, .. } => inner.map_column(last),
            ReaderView::Direct(node) => match &node.storage {
                NodeLayout::Struct { children, .. } => {
                    let child = if last { children.last() } else { children.first() }.ok_or_else(|| unsupported("empty Struct map reader"))?;
                    Ok(Self::direct(Arc::clone(child)))
                }
                NodeLayout::List { elements, .. } if matches!(node.field.data_type(), DataType::Map(_, _)) => Self::direct(Arc::clone(elements)).map_column(last),
                _ => Err(unsupported(if last { "mapValues" } else { "mapKeys" })),
            },
        }
    }
    fn list_offset(&self, index: PhysicalIndex, count: bool) -> Result<i32> {
        let row = self.address.translate(index)?;
        match &self.view {
            ReaderView::Direct(node) => node.list_offset(row, count),
            ReaderView::UnionLeg { union, inner, .. } => inner.list_offset(union.union_offset_index(row)?, count),
        }
    }
}

impl CellRef<'_> {
    /// Reads the signed element start without a parent-null guard from list/map or named-leg readers.
    /// Fixed lists compute the start by wrapping i32 multiplication without accessing row storage.
    /// Direct union and run-end wrappers reject the reference list-offset methods.
    pub fn list_start_index(&self) -> Result<i32> { self.vector.list_offset(PhysicalIndex::from_position(self.index)?, false) }

    /// Reads the signed element count under the same admission as list_start_index.
    /// Fixed lists return their declared size; variable lists read end before start and use wrapping subtraction.
    pub fn list_count(&self) -> Result<i32> { self.vector.list_offset(PhysicalIndex::from_position(self.index)?, true) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_ranges_keep_selection_topology_distinct_from_first_slice() {
        let memory = crate::MemoryBudget::new(1024 * 1024);
        let array: ArrayRef = Arc::new(arrow_array::Int64Array::from(vec![11, 23, 37]));
        let reservation = memory.reserve(array.get_array_memory_size()).unwrap();
        let vector = ExecutionVector::with_reservation(Arc::new(Field::new("i", DataType::Int64, false)), array, reservation).unwrap();
        let slice = vector.select(&Selection::Range { offset: 1, length: 2 }).unwrap();
        assert!(matches!(&slice.address, Address::Range { offset: 1, len: 2 }));
        let selected = slice.select(&Selection::Range { offset: 1, length: 1 }).unwrap();
        let Address::Indices { selection: first, .. } = &selected.address else { panic!() };
        assert_eq!(first.rows[0].0, 2);
        let repeated = selected.select(&Selection::All).unwrap();
        let Address::Indices { selection: second, .. } = &repeated.address else { panic!() };
        assert!(!Arc::ptr_eq(first, second));
        assert_eq!(second.rows[0].0, 2);
        assert_eq!(repeated.cell(0).unwrap().get_long(NullChecks::Enabled).unwrap(), 37);
    }
}
