//! Hybrid Julian/Gregorian fields for OpenJDK legacy dates.

/// Milliseconds in a calendar day before timezone offsets.
pub const MILLIS_PER_DAY: i64 = 86_400_000;
/// The first Gregorian day, October 15, 1582, relative to the Unix epoch.
pub const CUTOVER_DAY: i64 = -141_427;

/// Normalized hybrid calendar fields using an astronomical year, including year zero.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DateFields {
    pub year: i32,
    pub month: u8,
    pub day: u8,
    pub hour: u8,
    pub minute: u8,
    pub second: u8,
    pub millisecond: u16,
}

impl DateFields {
    /// Decodes signed Date milliseconds using the UTC calendar.
    pub fn from_local_millis(millis: i64) -> Self {
        Self::from_instant(millis, 0)
    }
    /// Decodes signed Date milliseconds using the supplied millisecond offset east of UTC.
    /// Normalization extends the fields past the signed Date endpoints when the offset crosses a boundary.
    pub fn from_instant(millis: i64, offset_millis: i32) -> Self {
        let clock = millis.rem_euclid(MILLIS_PER_DAY) + i64::from(offset_millis);
        let day = millis.div_euclid(MILLIS_PER_DAY) + clock.div_euclid(MILLIS_PER_DAY);
        let clock = clock.rem_euclid(MILLIS_PER_DAY);
        let (year, month, day) = date_from_epoch_day(day);
        Self { year, month, day, hour: (clock / 3_600_000) as u8, minute: (clock / 60_000 % 60) as u8,
            second: (clock / 1000 % 60) as u8, millisecond: (clock % 1000) as u16 }
    }
    /// Returns the 1-based year in the represented era.
    pub fn year_of_era(self) -> i32 { if self.year <= 0 { 1 - self.year } else { self.year } }
    /// Returns whether the astronomical year belongs to the era before 1 CE.
    pub fn before_common_era(self) -> bool { self.year <= 0 }
    /// Copies the fields into a lenient input with a zero-based month.
    pub fn raw(self) -> RawFields {
        RawFields { year: self.year, month0: i32::from(self.month) - 1, day: i32::from(self.day), hour: i32::from(self.hour),
            minute: i32::from(self.minute), second: i32::from(self.second), millisecond: i32::from(self.millisecond) }
    }
}

/// Lenient Java calendar input whose fields can overflow into adjacent months, days or clock units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RawFields {
    pub year: i32,
    pub month0: i32,
    pub day: i32,
    pub hour: i32,
    pub minute: i32,
    pub second: i32,
    pub millisecond: i32,
}

impl Default for RawFields {
    fn default() -> Self { Self { year: 1970, month0: 0, day: 1, hour: 0, minute: 0, second: 0, millisecond: 0 } }
}

impl RawFields {
    /// Resolves the fields to UTC Date milliseconds, wrapping at the signed 64-bit boundaries.
    pub fn local_millis(self) -> i64 {
        let clock = (((i64::from(self.hour) * 60 + i64::from(self.minute)) * 60 + i64::from(self.second)) * 1000) + i64::from(self.millisecond);
        let year = self.year.wrapping_add(self.month0.div_euclid(12));
        let month = self.month0.rem_euclid(12) as u8 + 1;
        let delta = i64::from(self.day) - 1 + clock.div_euclid(MILLIS_PER_DAY);
        let gregorian = gregorian_day(year, month, 1) + delta;
        let day = if self.year >= 1582 && gregorian >= CUTOVER_DAY { gregorian } else { julian_day(year, month, 1) + delta };
        day.wrapping_mul(MILLIS_PER_DAY).wrapping_add(clock.rem_euclid(MILLIS_PER_DAY))
    }
}

/// Converts a Gregorian date to a Unix epoch day using a 1-based month in `1..=12`.
pub fn gregorian_day(year: i32, month: u8, day: u8) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let y = year.rem_euclid(400);
    let m = i64::from(month) + if month > 2 { -3 } else { 9 };
    era * 146_097 + y * 365 + y / 4 - y / 100 + (153 * m + 2) / 5 + i64::from(day) - 1 - 719_468
}

/// Converts a Julian date to a Unix epoch day using a 1-based month in `1..=12`.
pub fn julian_day(year: i32, month: u8, day: u8) -> i64 {
    let year = i64::from(year) - i64::from(month <= 2);
    let month = i64::from(month) + if month > 2 { -3 } else { 9 };
    year * 365 + year.div_euclid(4) + (153 * month + 2) / 5 + i64::from(day) - 1 - 719_470
}

/// Converts a Unix epoch day in the legacy Date range to year, 1-based month and day of month.
pub fn date_from_epoch_day(day: i64) -> (i32,u8,u8) {
    let (year, day_of_year) = if day >= CUTOVER_DAY {
        let shifted = day + 719_468;
        let era = shifted.div_euclid(146_097);
        let day = shifted.rem_euclid(146_097);
        let y = (day - day / 1460 + day / 36_524 - day / 146_096) / 365;
        (era * 400 + y, day - (365 * y + y / 4 - y / 100))
    } else {
        let shifted = day + 719_470;
        let era = shifted.div_euclid(1461);
        let day = shifted.rem_euclid(1461);
        let y = (day - day / 1460) / 365;
        (era * 4 + y, day - 365 * y)
    };
    let month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month + 2) / 5 + 1;
    let month = month + if month < 10 { 3 } else { -9 };
    ((year + i64::from(month <= 2)) as i32, month as u8, day as u8)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn midnight(year: i32,month: i32,day: i32) -> i64 { RawFields { year,month0: month - 1,day,..Default::default() }.local_millis() }
    #[test]
    fn every_signed64_date_endpoint_retains_its_hybrid_fields_and_milliseconds() {
        for millis in [i64::MIN,i64::MIN + MILLIS_PER_DAY,-12_219_292_800_001,-12_219_292_800_000,-1,0,1,i64::MAX - MILLIS_PER_DAY,i64::MAX] {
            assert_eq!(DateFields::from_local_millis(millis).raw().local_millis(),millis);
        }
    }
    #[test]
    fn gregorian_cutover_skips_ten_labels_and_lenient_missing_dates_move_forward() {
        assert_eq!(midnight(1582,10,4) + MILLIS_PER_DAY,midnight(1582,10,15));
        assert_eq!(midnight(1582,10,10),midnight(1582,10,20));
        assert_eq!(midnight(1582,9,45),midnight(1582,10,15));
        let fields = DateFields::from_local_millis(midnight(1500,2,29));
        assert_eq!((fields.year,fields.month,fields.day),(1500,2,29));
        assert_eq!(midnight(1700,2,29),midnight(1700,3,1));
    }
    #[test]
    fn lenient_fields_preserve_eras_clock_carry_and_wrapping_date_storage() {
        for year in [0,-1,-400,1] {
            let fields = DateFields::from_local_millis(midnight(year,1,1));
            assert_eq!(fields.year,year);
            assert_eq!(fields.before_common_era(),year <= 0);
            assert_eq!(fields.year_of_era(),if year <= 0 { 1 - year } else { year });
        }
        assert_eq!(RawFields { year: 2000,month0: -1,day: 32,hour: -1,..Default::default() }.local_millis(),midnight(2000,1,1) - 3_600_000);
        let mut fields = DateFields::from_local_millis(i64::MIN).raw();
        fields.hour = 0;fields.minute = 0;fields.second = 0;fields.millisecond = 0;
        assert_eq!(fields.local_millis(),i64::MIN.wrapping_sub(i64::MIN.rem_euclid(MILLIS_PER_DAY)));
    }
    #[test]
    fn timezone_fields_extend_beyond_date_milliseconds_without_wrapping_the_calendar() {
        for (millis, offset) in [(i64::MAX, 86_399_000), (i64::MIN, -86_399_000)] {
            let fields = DateFields::from_instant(millis, offset);
            assert_eq!(fields.year.signum(), DateFields::from_local_millis(millis).year.signum());
            assert_eq!(fields.raw().local_millis(), millis.wrapping_add(i64::from(offset)));
            assert_ne!(fields, DateFields::from_local_millis(millis.wrapping_add(i64::from(offset))));
        }
    }
    #[test]
    fn lenient_month_overflow_keeps_the_original_year_calendar_selection() {
        assert_eq!(RawFields { year: 1581, month0: 24, ..Default::default() }.local_millis(), midnight(1583, 1, 11));
        assert_eq!(RawFields { year: 1583, month0: -24, ..Default::default() }.local_millis(), midnight(1581, 1, 1));
    }
}
