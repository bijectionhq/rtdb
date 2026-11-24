use ordered_logkeeper::ordered::AppendClass;

pub(super) fn chosen_result_at<T>(result: T, result_position: u64, decision_position: u64) -> Option<T> {
    if result_position == decision_position { Some(result) } else { None }
}

pub(super) fn matches_pending_append(
    result_class: AppendClass,
    pending_class: AppendClass,
    result_lsn: u64,
    expected_next: u64,
    writer_next: u64,
) -> bool {
    let same_class = matches!((result_class, pending_class),
        (AppendClass::Data, AppendClass::Data) | (AppendClass::Maintenance, AppendClass::Maintenance));
    same_class && result_lsn == expected_next && result_lsn == writer_next
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_selection_requires_the_original_decision_and_pending_record() {
        assert_eq!(chosen_result_at("original result", 9, 9), Some("original result"));
        assert_eq!(chosen_result_at("original result", 9, 8), None);
        assert!(matches_pending_append(AppendClass::Data, AppendClass::Data, 2, 2, 2));
        assert!(!matches_pending_append(AppendClass::Maintenance, AppendClass::Data, 2, 2, 2));
        assert!(!matches_pending_append(AppendClass::Data, AppendClass::Data, 2, 3, 2));
        assert!(!matches_pending_append(AppendClass::Data, AppendClass::Data, 2, 2, 3));
    }
}
