use super::*;

pub(super) fn bridge(target: &Schema, actual: &Schema, value: Avro, connect: &ConnectSchema, target_plan: &AvroDatumPlan, actual_plan: &AvroDatumPlan, next_union: &mut i32, context: &mut ConversionContext, depth: usize) -> Result<Avro> {
    if depth > 128 { return Err(error("avro-decode", "Connect output nesting limit exceeded")); }
    let (actual, value) = migration::unwrap(actual, value, &actual_plan.names)?;
    let target = resolve(target, &target_plan.names)?;
    let connect = connect.resolved();
    if matches!(value, Avro::Null) {
        return if connect.optional { Ok(value) } else { Err(error("avro-decode", "Resolved null targets a required Connect field")) };
    }
    match target {
        Schema::Union(union) => {
            let variants = union.variants();
            if (variants.len() == 1 && target_plan.unions.flatten_singleton) || (variants.len() == 2 && variants.iter().any(|schema| matches!(schema, Schema::Null))) {
                let index = variants.iter().position(|schema| !matches!(schema, Schema::Null)).ok_or_else(invalid)?;
                return Ok(Avro::Union(index as u32, Box::new(bridge(&variants[index], actual, value, &connect, target_plan, actual_plan, next_union, context, depth + 1)?)));
            }
            let SchemaKind::Struct(fields) = &connect.kind else { return Err(invalid()); };
            let cached_record = variants.iter().enumerate().find(|(_, member)| resolve(member, &target_plan.names).is_ok_and(|member| member == actual) && target_plan.names == actual_plan.names && target_plan.defaults == actual_plan.defaults)
                .and_then(|(index, member)| {
                    let name = target_plan.unions.field_name(resolve(member, &target_plan.names).ok()?, index);
                    fields.iter().find(|field| field.name == name).map(|field| Arc::clone(&field.schema))
                });
            let actual_connect = if cached_record.is_some() { cached_record } else if let Schema::Record(record) = actual {
                context.reserve(actual_plan.connect_allocation_bound(None)?)?;
                let raw = actual_plan.metadata.named(&record.name.fullname(None)).ok_or_else(invalid)?;
                let mut graph = graph::build_at(actual, raw, &actual_plan.names, &actual_plan.metadata, &actual_plan.defaults, actual_plan.unions, None, next_union)?;
                Arc::make_mut(&mut graph).optional = true;
                Some(graph)
            } else { None };
            for (index, (field, (variant_index, member))) in fields.iter().zip(variants.iter().enumerate().filter(|(_, schema)| !matches!(schema, Schema::Null))).enumerate() {
                let candidate = field.schema.resolved();
                let selected = actual_connect.as_deref().unwrap_or(&candidate);
                if union::matches_member(&candidate, selected, actual, &value, target_plan.unions, index)? {
                    return Ok(Avro::Union(variant_index as u32, Box::new(bridge(member, actual, value, &candidate, target_plan, actual_plan, next_union, context, depth + 1)?)));
                }
            }
            Err(error("avro-decode", "No writer Connect union member matches the resolved reader datum"))
        }
        Schema::Record(target) => {
            let (Schema::Record(actual), Avro::Record(values)) = (actual, value) else { return Err(invalid()); };
            let mut values = values.into_iter().map(|(_, value)| Some(value)).collect::<Vec<_>>();
            let mut output = Vec::with_capacity(target.fields.len());
            for field in &target.fields {
                let index = *actual.lookup.get(&field.name).ok_or_else(invalid)?;
                let actual = &actual.fields[index].schema;
                let value = values.get_mut(index).and_then(Option::take).ok_or_else(invalid)?;
                let child = match &connect.kind {
                    SchemaKind::Struct(fields) => fields.iter().find(|child| child.name == field.name).map(|child| child.schema.as_ref()),
                    SchemaKind::Map(key, value) => match field.name.as_str() { "key" => Some(key.as_ref()), "value" => Some(value.as_ref()), _ => None },
                    _ => None,
                }.ok_or_else(invalid)?;
                output.push((field.name.clone(), bridge(&field.schema, actual, value, child, target_plan, actual_plan, next_union, context, depth + 1)?));
            }
            Ok(Avro::Record(output))
        }
        Schema::Array(target) => {
            let Schema::Array(actual) = actual else { return Err(invalid()); };
            let child = match &connect.kind { SchemaKind::Array(child) => child.as_ref(), SchemaKind::Map(_, _) => connect.as_ref(), _ => return Err(invalid()) };
            let values = match value {
                Avro::PackedArray(packed) => {
                    if target.items == actual.items && (packed.values.is_empty() || depth + 1 + usize::from(packed.branches.is_some()) <= 128)
                        && (child.resolved().optional || packed.values.array().is_some_and(|array| array.logical_null_count() == 0))
                    {
                        return Ok(Avro::PackedArray(packed));
                    }
                    packed.into_physical_values()
                }
                Avro::Array(values) => values,
                _ => return Err(invalid()),
            };
            Ok(Avro::Array(values.into_iter().map(|value| bridge(&target.items, &actual.items, value, child, target_plan, actual_plan, next_union, context, depth + 1)).collect::<Result<_>>()?))
        }
        Schema::Map(target) => {
            let (Schema::Map(actual), Avro::Map(values)) = (actual, value) else { return Err(invalid()); };
            let SchemaKind::Map(_, child) = &connect.kind else { return Err(invalid()); };
            Ok(Avro::Map(values.map_visible_values(|value| bridge(&target.types, &actual.types, value, child, target_plan, actual_plan, next_union, context, depth + 1))?))
        }
        _ => Ok(value),
    }
}
fn invalid() -> rtdb_types::Anomaly { error("avro-decode", "Resolved reader datum does not match its writer Connect schema") }
