use rtdb_types::{LegacyTimeZone, Result};

use super::{date_locale::CalendarKind, error, legacy_calendar::{CUTOVER_DAY, MILLIS_PER_DAY, date_from_epoch_day, gregorian_day, julian_day}};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub(super) enum CalendarField {
    Era, Year, Month, WeekOfYear, WeekOfMonth, DayOfMonth, DayOfYear, DayOfWeek,
    DayOfWeekInMonth, AmPm, Hour, HourOfDay, Minute, Second, Millisecond, ZoneOffset, DstOffset,
}
use CalendarField::*;

const WEEK_YEAR: usize = 17;
const FIELD_COUNT: usize = 18;
const MIN_JAPANESE_YEAR: i32 = -292_275_055;
const ERA_STARTS: [(i32, u8, u8); 5] = [(1868, 1, 1), (1912, 7, 30), (1926, 12, 25), (1989, 1, 8), (2019, 5, 1)];

#[derive(Clone, Debug)]
pub(super) struct CalendarFields { values: [i32; FIELD_COUNT], stamps: [u32; FIELD_COUNT], next_stamp: u32 }
impl CalendarFields {
    pub fn new() -> Self { Self { values: [0; FIELD_COUNT], stamps: [0; FIELD_COUNT], next_stamp: 2 } }
    pub fn set(&mut self, field: CalendarField, value: i32) { self.set_index(field as usize, value); }
    pub fn set_week_year(&mut self, value: i32) { self.set_index(WEEK_YEAR, value); }
    pub fn add_year(&mut self, value: i32) {
        self.values[Year as usize] = self.values[Year as usize].wrapping_add(value);
        self.values[WEEK_YEAR] = self.values[WEEK_YEAR].wrapping_add(value);
    }
    pub fn clear(&mut self, field: CalendarField) { self.stamps[field as usize] = 0; self.values[field as usize] = 0; }
    pub fn is_set(&self, field: CalendarField) -> bool { self.stamp(field) != 0 }
    pub fn get(&self, field: CalendarField) -> i32 { self.values[field as usize] }
    fn value_or(&self, field: CalendarField, default: i32) -> i32 { if self.is_set(field) { self.get(field) } else { default } }
    fn stamp(&self, field: CalendarField) -> u32 { self.stamps[field as usize] }
    fn set_index(&mut self, field: usize, value: i32) {
        if self.next_stamp == u32::MAX {
            let old = self.stamps;
            for (index, stamp) in self.stamps.iter_mut().enumerate() {
                if old[index] != 0 { *stamp = 2 + old.iter().filter(|other| **other != 0 && **other < old[index]).count() as u32; }
            }
            self.next_stamp = self.stamps.iter().copied().max().unwrap_or(1).max(1) + 1;
        }
        self.values[field] = value; self.stamps[field] = self.next_stamp; self.next_stamp += 1;
    }

    // OpenJDK 25 Calendar.selectFields: only complete week groups compete until all complete groups are absent.
    fn selected(&self) -> Selection {
        let aggregate = |left: u32, right: u32| if left == 0 || right == 0 { 0 } else { left.max(right) };
        let dow = self.stamp(DayOfWeek);
        let mut dom = self.stamp(DayOfMonth);
        let mut wom = aggregate(self.stamp(WeekOfMonth), dow);
        let mut ordinal = aggregate(self.stamp(DayOfWeekInMonth), dow);
        let doy = self.stamp(DayOfYear);
        let mut woy = aggregate(self.stamp(WeekOfYear), dow);
        let mut best = dom.max(wom).max(ordinal).max(doy).max(woy);
        if best == 0 {
            wom = self.stamp(WeekOfMonth); ordinal = self.stamp(DayOfWeekInMonth).max(dow); woy = self.stamp(WeekOfYear);
            best = wom.max(ordinal).max(woy);
            if best == 0 { dom = self.stamp(Month); best = dom; }
        }
        let date = if best == dom || best == wom && self.stamp(WeekOfMonth) >= self.stamp(WeekOfYear)
            || best == ordinal && self.stamp(DayOfWeekInMonth) >= self.stamp(WeekOfYear) {
            if best == dom { DaySelection::MonthDay }
            else if wom == ordinal {
                if self.stamp(WeekOfMonth) >= self.stamp(DayOfWeekInMonth) { DaySelection::MonthWeek } else { DaySelection::MonthOrdinal }
            } else if best == wom { DaySelection::MonthWeek } else { DaySelection::MonthOrdinal }
        } else if best == doy { DaySelection::YearDay } else { DaySelection::YearWeek };
        let hour24 = self.stamp(HourOfDay);
        let hour12 = aggregate(self.stamp(Hour), self.stamp(AmPm));
        let best = hour24.max(hour12);
        let best = if best == 0 { self.stamp(Hour).max(self.stamp(AmPm)) } else { best };
        Selection { date, hour24: best != 0 && best == hour24 }
    }
}

#[derive(Clone, Copy)]
enum DaySelection { MonthDay, MonthWeek, MonthOrdinal, YearDay, YearWeek }
impl DaySelection { fn month(self) -> bool { matches!(self, Self::MonthDay | Self::MonthWeek | Self::MonthOrdinal) } }
struct Selection { date: DaySelection, hour24: bool }

#[derive(Clone, Copy, Debug)]
pub(super) struct CalendarSystem { kind: CalendarKind, first_day: u8, minimal_days: u8 }
#[derive(Clone, Copy, Debug)]
pub(super) struct CalendarView { fields: [i32; 17], week_year: i32 }
impl CalendarView {
    pub fn get(&self, field: CalendarField) -> i32 { self.fields[field as usize] }
    pub fn week_year(&self) -> i32 { self.week_year }
}

impl CalendarSystem {
    pub fn new(kind: CalendarKind, first_day: u8, minimal_days: u8) -> Result<Self> {
        if !(1..=7).contains(&first_day) || !(1..=7).contains(&minimal_days) { return Err(error("invalid-calendar-week-rules", "Calendar week rules must be between 1 and 7")); }
        Ok(Self { kind, first_day, minimal_days })
    }
    pub fn gregorian_compatible(self) -> bool { self.kind != CalendarKind::Japanese }
    pub fn week_date_supported(self) -> bool { self.gregorian_compatible() }
    pub fn view(self, millis: i64, zone: LegacyTimeZone) -> Result<CalendarView> {
        self.project(millis, zone.offsets_at_millis(millis)?, zone)
    }
    fn project(self, millis: i64, offsets: (i32, i32), zone: LegacyTimeZone) -> Result<CalendarView> {
        let (day, clock) = local_parts(millis, offsets.0.wrapping_add(offsets.1));
        let (year, month, dom) = if self.kind == CalendarKind::Japanese { gregorian_date(day) } else { date_from_epoch_day(day) };
        let era = if self.kind == CalendarKind::Japanese { japanese_era(day) as i32 } else { i32::from(year > 0) };
        let field_year = if self.kind == CalendarKind::Japanese {
            if era == 0 { year } else { year.wrapping_sub(ERA_STARTS[era as usize - 1].0).wrapping_add(1) }
        } else {
            let year = if era == 0 { 1i32.wrapping_sub(year) } else { year };
            year.wrapping_add(self.year_offset())
        };
        let (year_start, year_end, month_start) = if self.kind == CalendarKind::Japanese {
            let start = self.japanese_year_start(day, year, zone)?;
            let end = ERA_STARTS.iter().map(|&(y,m,d)| gregorian_day(y,m,d)).filter(|start| *start > day)
                .next().unwrap_or(i64::MAX).min(gregorian_day(year.wrapping_add(1),1,1));
            let month_start = if era > 0 && ERA_STARTS[era as usize - 1].0 == year && ERA_STARTS[era as usize - 1].1 == month {
                gregorian_day(year, month, ERA_STARTS[era as usize - 1].2)
            } else { gregorian_day(year,month,1) };
            (start,end,month_start)
        } else {
            let start = hybrid_year_start(year);
            let month_start = if year == 1582 && month == 10 { julian_day(year,month,1) } else { day - i64::from(dom) + 1 };
            (start,hybrid_year_start(year.wrapping_add(1)),month_start)
        };
        let doy = (day - year_start + 1) as i32;
        let mut woy = self.week_number(year_start, day);
        if woy == 0 {
            let previous = year_start - 1;
            let previous_start = if self.kind == CalendarKind::Japanese {
                let previous_year = gregorian_date(previous).0;
                if !ERA_STARTS.iter().any(|era| era.0 == year || era.0 == previous_year) {
                    year_start-365-i64::from(month_length(previous_year,1,true)==29)
                } else { self.japanese_year_start(previous,previous_year,zone)? }
            } else { hybrid_year_start(date_from_epoch_day(previous).0) };
            woy = self.week_number(previous_start,previous);
        } else if day >= self.week_start(year_end) && self.week_start(year_end) < year_end { woy = 1; }
        let mut fields = [0;17];
        for (field,value) in [(Era,era),(Year,field_year),(Month,i32::from(month)-1),(DayOfMonth,i32::from(dom)),
            (DayOfYear,doy),(DayOfWeek,weekday(day)),(WeekOfYear,woy),(WeekOfMonth,self.week_number(month_start,day)),
            (DayOfWeekInMonth,if self.kind == CalendarKind::Japanese { (i32::from(dom)-1)/7+1 } else { ((day-month_start)/7+1) as i32 }),
            (HourOfDay,(clock/3_600_000) as i32),(Hour,(clock/3_600_000%12) as i32),(AmPm,(clock/43_200_000) as i32),
            (Minute,(clock/60_000%60) as i32),(Second,(clock/1000%60) as i32),(Millisecond,(clock%1000) as i32),
            (ZoneOffset,offsets.0),(DstOffset,offsets.1)] { fields[field as usize] = value; }
        let week_year = if self.kind == CalendarKind::Japanese { field_year } else { self.week_year(&fields,year_start,year_end) };
        Ok(CalendarView { fields, week_year })
    }
    fn week_year(self, fields: &[i32;17], start: i64, end: i64) -> i32 {
        let mut visible = fields[Year as usize];
        if fields[Era as usize] == 0 { visible = 1i32.wrapping_sub(visible); }
        if visible > 1583 {
            if fields[Month as usize] == 0 && fields[WeekOfYear as usize] >= 52 { visible = visible.wrapping_sub(1); }
            else if fields[Month as usize] != 0 && fields[WeekOfYear as usize] == 1 { visible = visible.wrapping_add(1); }
            return visible;
        }
        let doy = fields[DayOfYear as usize];
        let maximum = (end-start) as i32;
        if doy > i32::from(self.minimal_days) && doy < maximum-6 { return visible; }
        let first = (i32::from(self.first_day)-weekday(start)).rem_euclid(7)+1;
        if doy < first {
            if first <= i32::from(self.minimal_days) { visible = visible.wrapping_sub(1); }
        } else {
            let next = self.gregorian_year(visible.wrapping_add(1), fields[Era as usize]);
            let next_start = hybrid_year_start(next);
            let mut delta = (i32::from(self.first_day)-weekday(next_start)).rem_euclid(7);
            if delta == 0 { delta = 7; }
            if delta >= i32::from(self.minimal_days) && maximum-doy+1 <= 7-delta { visible = visible.wrapping_add(1); }
        }
        visible
    }
    fn japanese_year_start(self, day: i64, year: i32, zone: LegacyTimeZone) -> Result<i64> {
        if year == MIN_JAPANESE_YEAR { return Ok(local_parts(i64::MIN,zone.offset_at_millis(i64::MIN)?).0); }
        let era = japanese_era(day);
        Ok(if era > 0 && ERA_STARTS[era-1].0 == year { let (_,m,d) = ERA_STARTS[era-1]; gregorian_day(year,m,d) } else { gregorian_day(year,1,1) })
    }
    fn year_offset(self) -> i32 { if self.kind == CalendarKind::Buddhist { 543 } else { 0 } }
    fn gregorian_year(self, year: i32, era: i32) -> i32 {
        let year = year.wrapping_sub(self.year_offset());
        if era == 0 { 1i32.wrapping_sub(year) } else { year }
    }
    fn week_start(self, start: i64) -> i64 {
        let first = on_or_before(start+6,i32::from(self.first_day));
        if first-start >= i64::from(self.minimal_days) { first-7 } else { first }
    }
    fn week_number(self, start: i64, day: i64) -> i32 { (day-self.week_start(start)).div_euclid(7) as i32 + 1 }

    pub fn resolve(self, fields: &CalendarFields, zone: LegacyTimeZone) -> Result<i64> {
        let week_date = fields.stamps[WEEK_YEAR] > fields.stamp(Year);
        if !week_date { return self.resolve_ordinary(fields,zone).map(|(millis,_)| millis); }
        if !self.week_date_supported() {
            let mut fields = fields.clone();
            if !fields.is_set(Year) { fields.set(Year,fields.values[WEEK_YEAR]); }
            return self.resolve_ordinary(&fields,zone).map(|(millis,_)| millis);
        }
        let (millis,offsets) = self.resolve_ordinary(fields,zone)?;
        let normalized = self.project(millis,offsets,zone)?;
        let mut week = fields.value_or(WeekOfYear,1);
        let mut dow = fields.value_or(DayOfWeek,i32::from(self.first_day));
        if !(1..=7).contains(&dow) {
            if dow >= 8 { dow -= 1; week = week.wrapping_add(dow/7); dow = dow%7+1; }
            else {
                let count = (1i64-i64::from(dow)+6)/7;
                dow = (i64::from(dow)+7*count) as i32; week = week.wrapping_sub(count as i32);
            }
            dow = if dow == 7 { 1 } else { dow+1 };
        }
        let mut first = CalendarFields::new();
        first.set(Era,normalized.get(Era)); first.set(Year,fields.values[WEEK_YEAR]); first.set(WeekOfYear,1); first.set(DayOfWeek,i32::from(self.first_day));
        let (first,_) = self.resolve_ordinary(&first,LegacyTimeZone::gmt())?;
        let days = (dow-i32::from(self.first_day)).rem_euclid(7).wrapping_add(week.wrapping_sub(1).wrapping_mul(7));
        let week_millis = first.wrapping_add(i64::from(days)*MILLIS_PER_DAY);
        let target = self.project(week_millis,(0,0),LegacyTimeZone::gmt())?;
        let mut replay = clock_fields(&normalized);
        replay.set(Era,target.get(Era));
        replay.set(Year,target.get(Year).wrapping_sub(self.year_offset()));
        replay.set(Month,target.get(Month)); replay.set(DayOfMonth,target.get(DayOfMonth));
        copy_offsets(fields,&mut replay);
        self.resolve_ordinary(&replay,zone).map(|(millis,_)| millis)
    }
    fn resolve_ordinary(self, fields: &CalendarFields, zone: LegacyTimeZone) -> Result<(i64,(i32,i32))> {
        let selected = fields.selected();
        let (year,era) = if self.kind == CalendarKind::Japanese {
            let era = fields.value_or(Era,if fields.is_set(Year) { 5 } else { 3 });
            if era > 5 { return Err(error("invalid-calendar-era", "Japanese calendar era exceeds the selected era table")); }
            (fields.value_or(Year,if fields.is_set(Era) { 1 } else { 45 }),era)
        } else {
            let era = fields.value_or(Era,1);
            if !(0..=1).contains(&era) { return Err(error("invalid-calendar-era", "Gregorian calendar era must be BCE or CE")); }
            (if fields.is_set(Year) { self.gregorian_year(fields.get(Year),era) } else if era == 0 { 1-1970 } else { 1970 },era)
        };
        let hour = if selected.hour24 { i64::from(fields.get(HourOfDay)) }
            else { i64::from(fields.get(Hour)) + i64::from(fields.get(AmPm).wrapping_mul(12)) };
        let clock = ((hour*60+i64::from(fields.get(Minute)))*60+i64::from(fields.get(Second)))*1000+i64::from(fields.get(Millisecond));
        let carry = clock.div_euclid(MILLIS_PER_DAY);
        let day = if self.kind == CalendarKind::Japanese { self.japanese_fixed(fields,selected.date,year,era,zone)?+carry }
            else { self.hybrid_fixed(fields,selected.date,year,carry) };
        let wall = day.wrapping_mul(MILLIS_PER_DAY).wrapping_add(clock.rem_euclid(MILLIS_PER_DAY));
        let mut offsets = if fields.is_set(ZoneOffset) && fields.is_set(DstOffset) { (0,0) } else { zone.offsets_by_wall_millis(wall)? };
        if fields.is_set(ZoneOffset) { offsets.0 = fields.get(ZoneOffset); }
        if fields.is_set(DstOffset) { offsets.1 = fields.get(DstOffset); }
        let millis = wall.wrapping_sub(i64::from(offsets.0.wrapping_add(offsets.1)));
        if !fields.is_set(ZoneOffset) || !fields.is_set(DstOffset) {
            let observed = zone.offsets_at_millis(millis)?;
            if !fields.is_set(ZoneOffset) { offsets.0 = observed.0; }
            if !fields.is_set(DstOffset) { offsets.1 = observed.1; }
        }
        Ok((millis,offsets))
    }
    fn hybrid_fixed(self, fields: &CalendarFields, selected: DaySelection, year: i32, carry: i64) -> i64 {
        let fixed = |gregorian| self.fixed(fields,selected,year,0,1,gregorian)+carry;
        if year > 1582 {
            let day = fixed(true);
            if day >= CUTOVER_DAY { return day; }
            return fixed(false);
        }
        if year < 1582 { return fixed(false); }
        let julian = fixed(false);
        if matches!(selected,DaySelection::YearDay|DaySelection::YearWeek) { return julian; }
        let gregorian = fixed(true);
        if gregorian >= CUTOVER_DAY { gregorian } else { julian }
    }
    fn fixed(self, fields: &CalendarFields, selected: DaySelection, mut year: i32, default_month: i32, first_day: i32, gregorian: bool) -> i64 {
        let mut month = if selected.month() { fields.get(Month) } else { default_month };
        year = year.wrapping_add(month.div_euclid(12)); month = month.rem_euclid(12);
        let day = if gregorian { gregorian_day(year,month as u8+1,first_day as u8) } else { julian_day(year,month as u8+1,first_day as u8) };
        self.relative_day(fields,selected,day,first_day,month,year)
    }
    fn relative_day(self, fields: &CalendarFields, selected: DaySelection, base: i64, first_day: i32, month: i32, year: i32) -> i64 {
        let dow = fields.value_or(DayOfWeek,i32::from(self.first_day));
        match selected {
            DaySelection::MonthDay => base+i64::from(fields.value_or(DayOfMonth,first_day))-i64::from(first_day),
            DaySelection::YearDay => base+i64::from(fields.get(DayOfYear))-1,
            DaySelection::MonthWeek|DaySelection::YearWeek => {
                let start = self.week_start(base);
                let start = if fields.is_set(DayOfWeek) { on_or_before(start+6,dow) } else { start };
                let weeks = if matches!(selected,DaySelection::MonthWeek) { i64::from(fields.get(WeekOfMonth).wrapping_sub(1).wrapping_mul(7)) }
                    else { 7*(i64::from(fields.get(WeekOfYear))-1) };
                start+weeks
            }
            DaySelection::MonthOrdinal => {
                let ordinal = fields.value_or(DayOfWeekInMonth,1);
                let end = if ordinal >= 0 { ordinal.wrapping_mul(7) }
                    else { month_length(year,month,self.kind == CalendarKind::Japanese).wrapping_add(ordinal.wrapping_add(1).wrapping_mul(7)) };
                on_or_before(base+i64::from(end)-1,dow)
            }
        }
    }
    fn japanese_fixed(self, fields: &CalendarFields, selected: DaySelection, mut year: i32, era: i32, zone: LegacyTimeZone) -> Result<i64> {
        let mut month = 0;
        let mut first = 1;
        if selected.month() {
            month = fields.get(Month);
            year = year.wrapping_add(month.div_euclid(12)); month = month.rem_euclid(12);
        } else if year == 1 && era != 0 {
            if era < 0 { return Err(error("invalid-calendar-era", "Negative Japanese era has no transition date")); }
            let (_,m,d) = ERA_STARTS[era as usize-1]; month = i32::from(m)-1; first = i32::from(d);
        }
        if year == MIN_JAPANESE_YEAR {
            let (day,_) = local_parts(i64::MIN,zone.offset_at_millis(i64::MIN)?);
            let (_,m,d) = gregorian_date(day);
            month = month.max(i32::from(m)-1);
            if month == i32::from(m)-1 { first = i32::from(d); }
        }
        let normalized_year = if era > 0 { ERA_STARTS[era as usize-1].0.wrapping_add(year).wrapping_sub(1) } else { year };
        let mut base = gregorian_day(normalized_year,month as u8+1,first as u8);
        if matches!(selected,DaySelection::YearDay) {
            let normalized = gregorian_date(base);
            if ERA_STARTS.iter().any(|era| era.0 == normalized.0) { base = self.japanese_year_start(base,normalized.0,zone)?; }
        }
        Ok(self.relative_day(fields,selected,base,first,month,year))
    }
    pub fn subtract_years(self, millis: i64, zone: LegacyTimeZone, years: i32) -> Result<i64> {
        if years == 0 { return Ok(millis); }
        let view = self.view(millis,zone)?;
        let (day,_) = local_parts(millis,zone.offset_at_millis(millis)?);
        let original_year = if self.kind == CalendarKind::Japanese { gregorian_date(day).0 } else { date_from_epoch_day(day).0 };
        let year = original_year.wrapping_sub(years);
        let month = view.get(Month);
        let mut dom = view.get(DayOfMonth).min(month_length(year,month,self.kind == CalendarKind::Japanese));
        let japanese_year = view.get(Year).wrapping_sub(years);
        if self.kind == CalendarKind::Japanese && japanese_year == MIN_JAPANESE_YEAR {
            let (minimum_day,minimum_clock) = local_parts(i64::MIN,zone.offset_at_millis(i64::MIN)?);
            let minimum_dom = i32::from(gregorian_date(minimum_day).2);
            let (_,clock) = local_parts(millis,zone.offset_at_millis(millis)?);
            let maximum = month_length(original_year.wrapping_add(400),month,true);
            dom = view.get(DayOfMonth).min(maximum).max(minimum_dom);
            if dom == minimum_dom && clock < minimum_clock { dom = (view.get(DayOfMonth)+1).min(maximum); }
        }
        let mut fields = clock_fields(&view);
        if self.kind == CalendarKind::Japanese { fields.set(Era,view.get(Era)); fields.set(Year,japanese_year); }
        else {
            fields.set(Era,i32::from(year>0));
            fields.set(Year,(if year>0 { year } else { 1i32.wrapping_sub(year) }).wrapping_add(self.year_offset()));
        }
        fields.set(Month,month); fields.set(DayOfMonth,dom);
        self.resolve_ordinary(&fields,zone).map(|(millis,_)|millis)
    }
}

fn clock_fields(view: &CalendarView) -> CalendarFields {
    let mut fields = CalendarFields::new();
    for field in [HourOfDay,Minute,Second,Millisecond] { fields.set(field,view.get(field)); }
    fields
}
fn copy_offsets(source: &CalendarFields, target: &mut CalendarFields) {
    for field in [ZoneOffset,DstOffset] { if source.is_set(field) { target.set(field,source.get(field)); } }
}
fn local_parts(millis: i64, offset: i32) -> (i64,i64) {
    let clock = millis.rem_euclid(MILLIS_PER_DAY)+i64::from(offset);
    (millis.div_euclid(MILLIS_PER_DAY)+clock.div_euclid(MILLIS_PER_DAY),clock.rem_euclid(MILLIS_PER_DAY))
}
fn weekday(day: i64) -> i32 { (day+4).rem_euclid(7) as i32+1 }
fn on_or_before(day: i64, weekday: i32) -> i64 { day-(day+719_163-i64::from(weekday.wrapping_sub(1))).rem_euclid(7) }
fn hybrid_year_start(year: i32) -> i64 { if year <= 1582 { julian_day(year,1,1) } else { gregorian_day(year,1,1) } }
fn japanese_era(day: i64) -> usize { ERA_STARTS.iter().take_while(|&&(year,month,date)| gregorian_day(year,month,date)<=day).count() }
fn month_length(year: i32, month: i32, proleptic: bool) -> i32 {
    let leap = year.rem_euclid(4)==0 && (!proleptic && year<=1582 || year.rem_euclid(100)!=0 || year.rem_euclid(400)==0);
    match month { 1 => if leap {29} else {28}, 3|5|8|10 =>30, _=>31 }
}
fn gregorian_date(day: i64) -> (i32,u8,u8) {
    let shifted = day+719_468;
    let era = shifted.div_euclid(146_097);
    let day = shifted.rem_euclid(146_097);
    let year = (day-day/1460+day/36_524-day/146_096)/365;
    let ordinal = day-(365*year+year/4-year/100);
    let month = (5*ordinal+2)/153;
    let date = ordinal-(153*month+2)/5+1;
    let month = month+if month<10 {3} else {-9};
    ((era*400+year+i64::from(month<=2)) as i32,month as u8,date as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::super::legacy_calendar::RawFields;

    fn system(kind: CalendarKind) -> CalendarSystem { CalendarSystem::new(kind,1,1).unwrap() }
    fn date(year: i32, month: i32, day: i32) -> i64 { RawFields { year,month0: month-1,day,..Default::default() }.local_millis() }
    fn fields(values: &[(CalendarField,i32)]) -> CalendarFields {
        let mut fields = CalendarFields::new();
        for &(field,value) in values { fields.set(field,value); }
        fields
    }
    fn resolve(calendar: CalendarSystem, values: &[(CalendarField,i32)]) -> i64 { calendar.resolve(&fields(values),LegacyTimeZone::gmt()).unwrap() }
    fn projected(calendar: CalendarSystem, millis: i64) -> CalendarView { calendar.view(millis,LegacyTimeZone::gmt()).unwrap() }

    #[test]
    fn full_date_endpoints_roundtrip_calendar_fields_without_local_millisecond_overflow() {
        for kind in [CalendarKind::Gregorian,CalendarKind::Buddhist,CalendarKind::Japanese] {
            let calendar = system(kind);
            for zone in [LegacyTimeZone::gmt(),LegacyTimeZone::parse("GMT+23:59:59").unwrap(),LegacyTimeZone::parse("GMT-23:59:59").unwrap()] {
                for millis in [i64::MIN,i64::MIN+MILLIS_PER_DAY,-1,0,1,i64::MAX-MILLIS_PER_DAY,i64::MAX] {
                    let view = calendar.view(millis,zone).unwrap();
                    let mut fields = clock_fields(&view);
                    for field in [Era,Year,Month,DayOfMonth,ZoneOffset,DstOffset] { fields.set(field,view.get(field)); }
                    assert_eq!(calendar.resolve(&fields,zone).unwrap(),millis,"{kind:?} {zone} {millis}");
                    assert!((0..=999).contains(&view.get(Millisecond)));
                    assert!((1..=7).contains(&view.get(DayOfWeek)));
                }
            }
        }
    }

    #[test]
    fn hybrid_cutover_counts_existing_days_and_normalizes_missing_date_labels() {
        let calendar = system(CalendarKind::Gregorian);
        let before = projected(calendar,date(1582,10,4));
        let after = projected(calendar,date(1582,10,15));
        assert_eq!((before.get(DayOfYear),after.get(DayOfYear)),(277,278));
        assert_eq!((after.get(DayOfWeek),after.get(DayOfWeekInMonth),after.get(WeekOfMonth)),(6,1,1));
        assert_eq!(projected(calendar,date(1582,12,31)).get(DayOfYear),355);
        assert_eq!(resolve(calendar,&[(Year,1582),(DayOfYear,278)]),date(1582,10,15));
        assert_eq!(resolve(calendar,&[(Year,1582),(Month,9),(DayOfMonth,10)]),date(1582,10,20));
        assert_eq!(resolve(calendar,&[(Year,1581),(Month,24),(DayOfMonth,1)]),date(1583,1,11));
        assert_eq!(resolve(calendar,&[(Year,1583),(Month,-24),(DayOfMonth,1)]),date(1581,1,1));
        assert_eq!(resolve(calendar,&[(Year,1500),(Month,1),(DayOfMonth,29)]),date(1500,2,29));
    }

    #[test]
    fn buddhist_years_shift_year_of_era_and_year_addition_uses_gregorian_leap_rules() {
        let calendar = system(CalendarKind::Buddhist);
        for (year,era,buddhist) in [(2024,1,2567),(1,1,544),(0,0,544),(-1,0,545)] {
            let view = projected(calendar,date(year,1,1));
            assert_eq!((view.get(Era),view.get(Year)),(era,buddhist));
            assert_eq!(resolve(calendar,&[(Era,era),(Year,buddhist)]),date(year,1,1));
        }
        assert_eq!(resolve(calendar,&[(Era,0),(Year,543)]),date(1,1,1));
        assert_eq!(calendar.resolve(&CalendarFields::new(),LegacyTimeZone::gmt()).unwrap(),0);
        assert_eq!(calendar.subtract_years(date(2024,2,29)+12_345,LegacyTimeZone::gmt(),1).unwrap(),date(2023,2,28)+12_345);
        assert_eq!(calendar.subtract_years(date(2000,2,29),LegacyTimeZone::gmt(),80).unwrap(),date(1920,2,29));
        assert_eq!(calendar.subtract_years(date(1,3,1),LegacyTimeZone::gmt(),1).unwrap(),date(0,3,1));
    }

    #[test]
    fn japanese_calendar_uses_proleptic_dates_signed_pre_meiji_years_and_real_era_boundaries() {
        let calendar = system(CalendarKind::Japanese);
        assert!(!calendar.week_date_supported());
        assert!(!calendar.gregorian_compatible());
        for (era,millis) in [(1,-3_218_832_000_000),(2,-1_812_153_600_000),(3,-1_357_603_200_000),(4,600_220_800_000),(5,1_556_668_800_000)] {
            let first = projected(calendar,millis);
            assert_eq!((first.get(Era),first.get(Year),first.get(DayOfYear)),(era,1,1));
            assert_eq!(projected(calendar,millis-1).get(Era),era-1);
        }
        for year in [-1,0,1,1500,1867] {
            let millis = gregorian_day(year,1,1)*MILLIS_PER_DAY;
            let view = projected(calendar,millis);
            assert_eq!((view.get(Era),view.get(Year)),(0,year));
            assert_eq!(resolve(calendar,&[(Era,0),(Year,year)]),millis);
        }
        assert_eq!(resolve(calendar,&[(Era,0),(Year,1500),(Month,1),(DayOfMonth,29)]),gregorian_day(1500,3,1)*MILLIS_PER_DAY);
        let reiwa = projected(calendar,1_556_668_800_000);
        assert_eq!((reiwa.get(Month),reiwa.get(DayOfMonth),reiwa.get(DayOfWeekInMonth)),(4,1,1));
        let heisei = projected(calendar,1_556_668_799_999);
        assert_eq!((heisei.get(Era),heisei.get(Year),heisei.get(DayOfYear),heisei.get(WeekOfYear)),(4,31,120,1));
        let taisho = projected(calendar,-1_812_153_600_000);
        assert_eq!((taisho.get(DayOfMonth),taisho.get(DayOfWeekInMonth),taisho.get(WeekOfMonth)),(30,5,1));
    }

    #[test]
    fn japanese_unset_year_and_selected_day_fields_keep_calendar_defaults() {
        let calendar = system(CalendarKind::Japanese);
        assert_eq!(calendar.resolve(&CalendarFields::new(),LegacyTimeZone::gmt()).unwrap(),0);
        assert_eq!(resolve(calendar,&[(Year,1)]),date(2019,1,1));
        assert_eq!(resolve(calendar,&[(Era,5)]),date(2019,1,1));
        assert_eq!(resolve(calendar,&[(Era,5),(Year,1),(DayOfYear,1)]),date(2019,5,1));
        assert_eq!(resolve(calendar,&[(Era,5),(Year,1),(Month,0),(DayOfMonth,1)]),date(2019,1,1));
        assert_eq!(resolve(calendar,&[(Era,-1),(Year,2000),(Month,0),(DayOfMonth,1)]),date(2000,1,1));
        assert!(calendar.resolve(&fields(&[(Era,6)]),LegacyTimeZone::gmt()).is_err());
        assert!(calendar.resolve(&fields(&[(Era,-1),(Year,1),(DayOfYear,1)]),LegacyTimeZone::gmt()).is_err());
        let mut week = CalendarFields::new(); week.set_week_year(2);
        assert_eq!(calendar.resolve(&week,LegacyTimeZone::gmt()).unwrap(),date(2020,1,1));
    }

    #[test]
    fn japanese_negative_month_ordinals_use_the_supplied_era_year_for_month_length() {
        let japanese = system(CalendarKind::Japanese);
        assert_eq!(resolve(japanese,&[(Era,5),(Year,2),(Month,1),(DayOfWeekInMonth,-1),(DayOfWeek,7)]),date(2020,2,22));
        let gregorian = system(CalendarKind::Gregorian);
        assert_eq!(resolve(gregorian,&[(Year,2020),(Month,1),(DayOfWeekInMonth,-1),(DayOfWeek,7)]),date(2020,2,29));
    }

    #[test]
    fn us_and_iso_week_rollover_and_japanese_short_years_follow_locale_rules() {
        let us = system(CalendarKind::Gregorian);
        let iso = CalendarSystem::new(CalendarKind::Gregorian,2,4).unwrap();
        let view = projected(us,date(2016,1,1));
        assert_eq!((view.get(WeekOfYear),view.week_year()),(1,2016));
        let view = projected(iso,date(2016,1,1));
        assert_eq!((view.get(WeekOfYear),view.week_year()),(53,2015));
        let view = projected(iso,date(2014,12,29));
        assert_eq!((view.get(WeekOfYear),view.week_year()),(1,2015));
        let view = projected(iso,date(2016,1,4));
        assert_eq!((view.get(WeekOfYear),view.week_year()),(1,2016));
        let japanese = CalendarSystem::new(CalendarKind::Japanese,2,4).unwrap();
        let view = projected(japanese,date(1926,12,25));
        assert_eq!((view.get(DayOfYear),view.get(WeekOfYear),view.get(WeekOfMonth),view.get(DayOfWeekInMonth)),(1,51,0,4));
        let minimum = projected(japanese,i64::MIN);
        assert_eq!(minimum.get(DayOfYear),1);
        assert!((1..=53).contains(&minimum.get(WeekOfYear)));
    }

    #[test]
    fn field_stamps_choose_complete_groups_and_keep_month_out_of_day_precedence() {
        let calendar = system(CalendarKind::Gregorian);
        let mut values = fields(&[(Year,2020),(Month,0),(DayOfMonth,15),(DayOfYear,60),(Month,1)]);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,2,29));
        values.set(DayOfMonth,1);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,2,1));
        values.set(WeekOfMonth,3);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,2,1));
        values.set(DayOfWeek,2);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,2,10));
        let mut values = fields(&[(Year,2021),(Month,0),(WeekOfMonth,2),(WeekOfYear,10),(DayOfWeek,2)]);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2021,3,1));
        values.set(WeekOfMonth,2);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2021,1,4));
        values.clear(WeekOfYear); values.clear(WeekOfMonth);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2021,1,4));
    }

    #[test]
    fn clock_stamps_year_replay_and_lenient_integer_arithmetic_are_preserved() {
        let calendar = system(CalendarKind::Gregorian);
        let mut values = fields(&[(Hour,3),(AmPm,1),(HourOfDay,5)]);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),5*3_600_000);
        values.set(AmPm,0);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),3*3_600_000);
        assert_eq!(resolve(calendar,&[(AmPm,i32::MAX)]),-12*3_600_000);
        let first_week = date(1969,12,28);
        assert_eq!(resolve(calendar,&[(WeekOfYear,i32::MAX)]),first_week+7*(i64::from(i32::MAX)-1)*MILLIS_PER_DAY);
        assert_eq!(resolve(calendar,&[(WeekOfMonth,i32::MAX)]),first_week+i64::from(i32::MAX.wrapping_sub(1).wrapping_mul(7))*MILLIS_PER_DAY);
        assert_eq!(resolve(calendar,&[(Year,2000),(Month,-1),(DayOfMonth,32),(HourOfDay,-1)]),date(2000,1,1)-3_600_000);
        let mut values = fields(&[(Year,1900),(Month,1),(DayOfMonth,29)]); values.add_year(100);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2000,2,29));
        let mut values = fields(&[(Year,2020),(HourOfDay,25)]); values.set_week_year(2020); values.set(WeekOfYear,1); values.set(DayOfWeek,1);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2019,12,29)+3_600_000);
        values.add_year(1);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,12,27)+3_600_000);
    }

    #[test]
    fn week_date_replay_preserves_buddhist_virtual_year_set_and_weekday_normalization() {
        let buddhist = system(CalendarKind::Buddhist);
        let mut values = CalendarFields::new(); values.set_week_year(2563); values.set(WeekOfYear,1); values.set(DayOfWeek,1);
        assert_eq!(buddhist.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(1476,12,29));
        let gregorian = system(CalendarKind::Gregorian);
        values.set_week_year(2020); values.set(DayOfWeek,8);
        assert_eq!(gregorian.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,1,6));
        values.set(DayOfWeek,0);
        assert_eq!(gregorian.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2019,12,22));
        values.set(Year,2021);
        assert_eq!(gregorian.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2021,1,2));
    }

    #[test]
    fn explicit_standard_and_dst_fields_override_wall_components_independently() {
        let calendar = system(CalendarKind::Gregorian);
        let zone = LegacyTimeZone::parse("Europe/Berlin").unwrap();
        let mut values = fields(&[(Year,2024),(Month,2),(DayOfMonth,31),(HourOfDay,2),(Minute,30)]);
        let midnight = date(2024,3,31);
        assert_eq!(calendar.resolve(&values,zone).unwrap(),midnight+90*60_000);
        values.set(ZoneOffset,0);
        assert_eq!(calendar.resolve(&values,zone).unwrap(),midnight+150*60_000);
        values.clear(ZoneOffset); values.set(DstOffset,7_200_000);
        assert_eq!(calendar.resolve(&values,zone).unwrap(),midnight-30*60_000);
        assert_eq!(calendar.resolve(&fields(&[(ZoneOffset,i32::MAX),(DstOffset,1)]),zone).unwrap(),2_147_483_648);
    }

    #[test]
    fn stamp_compaction_preserves_latest_selection_without_unbounded_state() {
        let calendar = system(CalendarKind::Gregorian);
        let mut values = fields(&[(Year,2020),(DayOfYear,60),(Month,0),(DayOfMonth,1)]);
        values.next_stamp = u32::MAX;
        values.set(DayOfYear,61);
        assert_eq!(calendar.resolve(&values,LegacyTimeZone::gmt()).unwrap(),date(2020,3,1));
        assert!(values.next_stamp < 20);
        assert!(CalendarSystem::new(CalendarKind::Gregorian,0,1).is_err());
        assert!(CalendarSystem::new(CalendarKind::Gregorian,1,8).is_err());
    }

    #[test]
    fn century_subtraction_crosses_japanese_eras_and_wraps_at_raw_date_limits() {
        let japanese = system(CalendarKind::Japanese);
        assert_eq!(japanese.subtract_years(date(2019,5,1),LegacyTimeZone::gmt(),80).unwrap(),date(1939,5,1));
        let gregorian = system(CalendarKind::Gregorian);
        for millis in [i64::MIN,i64::MAX] {
            let mut expected = super::super::legacy_calendar::DateFields::from_local_millis(millis).raw();
            expected.year = expected.year.wrapping_sub(80);
            expected.day = expected.day.min(month_length(expected.year,expected.month0,false));
            assert_eq!(gregorian.subtract_years(millis,LegacyTimeZone::gmt(),80).unwrap(),expected.local_millis());
        }
    }
}
