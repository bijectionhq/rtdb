//! `TimeZone`'s form of each zone's rules: OpenJDK `ZoneInfoFile`'s 1900–2100 transition table and its SimpleTimeZone tail.

use super::{Rule,Rules};

#[derive(Debug)]
pub struct LegacyRules { pub raw: i32,pub last_standard_transition: Option<i64>,pub transitions: Vec<(i64,i32,i32)>,pub future: Option<LegacyRecurring> }
#[derive(Debug)]
pub struct LegacyRecurring { pub start: LegacyTransition,pub end: LegacyTransition,pub saving: i32 }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub struct LegacyTransition { pub month: u8,pub day: i32,pub weekday: u8,pub last: bool,pub second: u32,pub definition: u8 }

const UTC_1900: i64 = -2_208_988_800;
const UTC_2100_END: i64 = 4_133_980_799;
const LOCAL_2100_START: i64 = 4_102_444_800;

pub(super) fn allocation_entries(rules: &Rules) -> Result<usize,String> {
    let base = rules.transitions.len() + rules.standards.len() + 2;
    let Some(last) = rules.transitions.len().checked_sub(1) else { return Ok(base); };
    if rules.recurring.len() < 2 { return Ok(base); }
    let year = date(rules.transitions[last].0,before(rules,last)).0;
    let years = usize::try_from((2100i64 - i64::from(year)).max(0)).map_err(|_|"Legacy TZDB recurrence span exceeds addressable memory")?;
    years.checked_mul(rules.recurring.len()).and_then(|count|count.checked_add(base)).ok_or_else(||"Legacy TZDB table bound overflow".into())
}

fn day(year: i32,month: u8,date: i32) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);let y = year.rem_euclid(400);
    let month = i64::from(month) + if month > 2 { -3 } else { 9 };
    era * 146_097 + y * 365 + y / 4 - y / 100 + (153 * month + 2) / 5 + i64::from(date) - 1 - 719_468
}
fn date(second: i64,offset: i32) -> (i32,u8,u8,u32) {
    let local = second + i64::from(offset);
    let shifted = local.div_euclid(86_400) + 719_468;
    let era = shifted.div_euclid(146_097);let d = shifted.rem_euclid(146_097);
    let y = (d - d / 1460 + d / 36_524 - d / 146_096) / 365;
    let d = d - (365 * y + y / 4 - y / 100);
    let month = (5 * d + 2) / 153;let d = d - (153 * month + 2) / 5 + 1;
    let month = month + if month < 10 { 3 } else { -9 };
    ((era * 400 + y + i64::from(month <= 2)) as i32,month as u8,d as u8,local.rem_euclid(86_400) as u32)
}
fn month_length(year: i32,month: u8) -> i32 {
    match month { 2 => if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 29 } else { 28 },4 | 6 | 9 | 11 => 30,_ => 31 }
}
fn transition(rule: &Rule,year: i32) -> i64 {
    let dom = if rule.day < 0 { month_length(year,rule.month) + 1 + i32::from(rule.day) } else { i32::from(rule.day) };
    let mut date = day(year,rule.month,dom);
    if rule.weekday != 0 {
        let difference = i64::from(rule.weekday) - (date + 3).rem_euclid(7) - 1;
        date += if rule.day < 0 { -(-difference).rem_euclid(7) } else { difference.rem_euclid(7) };
    }
    if rule.end_of_day { date += 1; }
    date * 86_400 + i64::from(rule.second) - i64::from(match rule.definition { 0 => 0,1 => rule.before,_ => rule.standard })
}
fn standard(rules: &Rules,second: i64) -> i32 {
    let index = rules.standards.partition_point(|(time,_)|*time <= second);
    if index == 0 { rules.standard_initial } else { rules.standards[index - 1].1 }
}
fn before(rules: &Rules,index: usize) -> i32 { if index == 0 { rules.initial } else { rules.transitions[index - 1].1 } }
struct RuleTable { offsets: Vec<i32>, count: usize, packed: Vec<i64> }
impl RuleTable {
    fn new() -> Self { Self { offsets: vec![0; 16], count: 0, packed: Vec::new() } }
    fn initial_raw(&mut self, raw: i32) { self.offsets[0] = raw * 1000; self.count = 1; }
    fn offset_index(&mut self, from: usize, seconds: i32) -> usize {
        let millis = seconds * 1000;
        let mut index = from;
        while index < self.count && self.offsets[index] != millis { index += 1; }
        if index >= self.offsets.len() { self.offsets.resize(index + 1, 0); }
        self.offsets[index] = millis;
        if index == self.count { self.count += 1; }
        index
    }
    fn append(&mut self, second: i64, total: i32, standard: i32) {
        let offset = self.offset_index(0, total);
        let saving = if total == standard { 0 } else { self.offset_index(1, total - standard) };
        self.packed.push(second.wrapping_mul(1000).wrapping_shl(12) | (((saving & 15) as i64) << 4) | (offset & 15) as i64);
    }
    fn rows(&self) -> impl Iterator<Item = (i64, i32, i32)> + '_ {
        self.packed.iter().map(|packed| {
            let saving = ((packed >> 4) & 15) as usize;
            (packed >> 12, self.offsets[(packed & 15) as usize], if saving == 0 { 0 } else { self.offsets[saving] })
        })
    }
}

impl LegacyTransition {
    fn of(rule: &Rule) -> Self {
        Self { month: rule.month,day: i32::from(rule.day),weekday: rule.weekday,last: rule.weekday != 0 && (rule.day < 0 || rule.day >= 24),second: if rule.end_of_day { 86_400 } else { rule.second },definition: rule.definition }
    }
    fn exact(second: i64,offset: i32) -> Self {
        let (_,month,day,second) = date(second,offset);
        Self { month,day: i32::from(day),weekday: 0,last: false,second,definition: 1 }
    }
}

pub(super) fn build(rules: &Rules) -> LegacyRules {
    let raw = rules.standards.last().map_or(rules.standard_initial,|(_,offset)|*offset);
    let mut table = RuleTable::new();
    let mut future = None;
    if !rules.transitions.is_empty() {
        let last = rules.transitions.len() - 1;
        let mut last_year = date(rules.transitions[last].0,before(rules,last)).0;
        let mut index = rules.transitions.partition_point(|(second,_)|*second < UTC_1900);
        let mut standard_index = 1;
        if index < rules.transitions.len() {
            table.initial_raw(raw);
            table.append(UTC_1900,before(rules,index),standard(rules,UTC_1900));
        }
        while index < rules.transitions.len() {
            let (second,offset) = rules.transitions[index];
            if second > UTC_2100_END { last_year = 2100;break; }
            while standard_index < rules.standards.len() {
                let (time,standard_offset) = rules.standards[standard_index];
                if time >= UTC_1900 {
                    if time > second { break; }
                    if time < second { table.append(time,before(rules,index),standard_offset); }
                }
                standard_index += 1;
            }
            table.append(second,offset,standard(rules,second));
            index += 1;
        }
        for &(second,_) in &rules.standards[standard_index.min(rules.standards.len())..] {
            if second >= UTC_1900 { let offset = before(rules,index);table.append(second,offset,offset); }
        }
        if rules.recurring.len() > 1 {
            while last_year < 2100 {
                last_year += 1;
                for rule in &rules.recurring { table.append(transition(rule,last_year),rule.after,rule.standard); }
            }
            let mut start = &rules.recurring[rules.recurring.len() - 2];
            let mut end = &rules.recurring[rules.recurring.len() - 1];
            if start.after < start.before && end.after > end.before { std::mem::swap(&mut start,&mut end); }
            future = Some(LegacyRecurring { start: LegacyTransition::of(start),end: LegacyTransition::of(end),saving: (start.after - start.before) * 1000 });
        } else if !table.packed.is_empty() {
            if last_year < 2100 { table.append(LOCAL_2100_START - i64::from(raw),raw,raw); }
            else if rules.transitions.len() > 2 {
                let a = rules.transitions.len() - 2;let b = a + 1;
                let (start_time,start_offset) = rules.transitions[a];let (end_time,end_offset) = rules.transitions[b];
                let start_standard = standard(rules,start_time);let end_standard = standard(rules,end_time);
                if start_offset > start_standard && end_offset == end_standard {
                    let start_before = before(rules,a);let end_before = before(rules,b);
                    let start = LegacyTransition::exact(start_time,start_before.min(start_offset));
                    let end = LegacyTransition::exact(end_time,end_before.max(end_offset));
                    future = Some(LegacyRecurring { start,end,saving: (start_offset - start_standard) * 1000 });
                }
            }
        }
    }
    LegacyRules { raw: raw * 1000,last_standard_transition: rules.standards.last().map(|(second,_)|*second),transitions: table.rows().collect(),future }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recurring() -> Vec<Rule> {
        vec![Rule { month: 3,day: 25,weekday: 7,second: 3600,end_of_day: false,definition: 0,standard: 3600,before: 3600,after: 7200 },
            Rule { month: 10,day: 25,weekday: 7,second: 3600,end_of_day: false,definition: 0,standard: 3600,before: 7200,after: 3600 }]
    }
    #[test]
    fn legacy_recurring_rule_bounds_are_checked_without_expanding_the_calendar_span() {
        let rules = Rules { initial: 3600,fixed: false,standard_initial: 3600,standards: vec![],transitions: vec![(day(-999_999_998,1,1) * 86_400,3600)],recurring: recurring() };
        assert!(allocation_entries(&rules).unwrap() > 1_048_576);
    }
    #[test]
    fn legacy_tail_uses_last_weekday_conversion_and_preserves_final_standard_offset() {
        let rules = Rules { initial: 561,fixed: false,standard_initial: 561,standards: vec![(0,3600)],transitions: vec![(0,3600)],recurring: recurring() };
        let legacy = build(&rules);
        assert_eq!(legacy.raw,3_600_000);
        let future = legacy.future.unwrap();
        assert_eq!((future.start.day,future.start.weekday,future.start.last),(25,7,true));
        assert_eq!(future.saving,3_600_000);
        assert_eq!(legacy.transitions[0],(-2_208_988_800_000,561_000,0));
    }
    #[test]
    fn packed_legacy_transition_offsets_use_the_reference_four_bit_indexes() {
        let mut table = RuleTable::new();
        table.initial_raw(0);
        for offset in 1..=17 { table.append(i64::from(offset), offset, offset); }
        let rows = table.rows().collect::<Vec<_>>();
        assert_eq!(rows[14], (15_000, 15_000, 0));
        assert_eq!(rows[15], (16_000, 0, 0));
        assert_eq!(rows[16], (17_000, 1_000, 0));
    }
    #[test]
    fn legacy_tail_retains_end_of_day_as_twenty_four_hours() {
        let mut rules = recurring();
        rules[0].second = 0;
        rules[0].end_of_day = true;
        assert_eq!(LegacyTransition::of(&rules[0]).second, 86_400);
        let rules = Rules { initial: 3600, fixed: false, standard_initial: 3600, standards: vec![], transitions: vec![(0, 3600)], recurring: rules };
        let start = build(&rules).future.unwrap().start;
        assert_eq!(start, LegacyTransition { month: 3, day: 25, weekday: 7, last: true, second: 86_400, definition: 0 });
    }
}
