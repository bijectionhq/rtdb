use super::*;

#[derive(Debug,PartialEq,Eq)]
pub(crate) enum ArrowKind {
    Bool,Int { width: i32,signed: bool },Floating(bool),Utf8,Binary,FixedSizeBinary(i32),Decimal { precision: i32,scale: i32,width: i32 },
    Date(bool),Time { unit: Unit,width: i32 },Timestamp { unit: Unit,timezone: Option<Arc<str>> },Duration(Unit),Interval(Interval),
    Struct,List,Map(bool),FixedSizeList(i32),Union(bool),Extension(Extension),
}
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Unit { Second,Milli,Micro,Nano }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Interval { YearMonth,DayTime,MonthDayNano }
#[derive(Clone,Copy,Debug,PartialEq,Eq)]
pub(crate) enum Extension { Keyword,Transit,Set,TsTzRange,IntervalMDM,Uuid,Uri,Oid,RegClass,RegProc }

impl ArrowKind {
    pub(super) fn read(spec: &Object,scope: &EvaluationScope) -> Result<Self> {
        match spec { Object::Keyword(name) => Self::parse(name,None,scope),Object::Vector(values) | Object::List(values) => Self::sequence(values,scope),_ => Err(invalid()) }
    }
    pub(super) fn sequence(values: &[Object],scope: &EvaluationScope) -> Result<Self> {
        let Some(Object::Keyword(name)) = values.first() else { return Err(invalid()); };Self::parse(name,Some(&values[1..]),scope)
    }
    fn parse(name: &str,tail: Option<&[Object]>,scope: &EvaluationScope) -> Result<Self> {
        let args = ||tail.ok_or_else(invalid);
        Ok(match name {
            "bool" => Self::Bool,"i8" => Self::Int { width: 8,signed: true },"i16" => Self::Int { width: 16,signed: true },"i32" => Self::Int { width: 32,signed: true },"i64" => Self::Int { width: 64,signed: true },
            "f32" => Self::Floating(false),"f64" => Self::Floating(true),"utf8" => Self::Utf8,"varbinary" => Self::Binary,
            "iid" => Self::FixedSizeBinary(16),"instant" => Self::Timestamp { unit: Unit::Micro,timezone: Some(text_arc("UTC",scope)?) },
            "struct" => Self::Struct,"list" => Self::List,"union" => Self::Union(true),"sparse-union" => Self::Union(false),
            "keyword" => Self::Extension(Extension::Keyword),"transit" => Self::Extension(Extension::Transit),"set" => Self::Extension(Extension::Set),"tstz-range" => Self::Extension(Extension::TsTzRange),
            "uuid" => Self::Extension(Extension::Uuid),"uri" => Self::Extension(Extension::Uri),"oid" => Self::Extension(Extension::Oid),"regclass" => Self::Extension(Extension::RegClass),"regproc" => Self::Extension(Extension::RegProc),
            "fixed-size-binary" => Self::FixedSizeBinary(int(arg(args()?,0),false,scope)?),"fixed-size-list" => Self::FixedSizeList(int(arg(args()?,0),false,scope)?),
            "decimal" => {
                let args = args()?;
                let precision = int(arg(args,0),false,scope)?;let scale = int(arg(args,1),false,scope)?;let width = int(arg(args,2),true,scope)?;
                Self::Decimal { precision,scale,width }
            }
            "date" => Self::Date(match key(arg(args()?,0))? { "day" => false,"milli" => true,_ => return Err(invalid()) }),
            "time-local" => { let unit = Unit::read(arg(args()?,0))?;Self::Time { unit,width: if matches!(unit,Unit::Second | Unit::Milli) { 32 } else { 64 } } }
            "timestamp-tz" | "timestamp-local" => {
                let args = args()?;let unit = Unit::read(arg(args,0))?;
                let timezone = if name == "timestamp-local" { None } else { match arg(args,1) { Object::Null => None,Object::String(value) => Some(text_arc(value,scope)?),_ => return Err(invalid()) } };
                Self::Timestamp { unit,timezone }
            }
            "duration" => Self::Duration(Unit::read(arg(args()?,0))?),
            "interval" => match key(arg(args()?,0))? {
                "year-month" => Self::Interval(Interval::YearMonth),"day-time" => Self::Interval(Interval::DayTime),"month-day-nano" => Self::Interval(Interval::MonthDayNano),"month-day-micro" => Self::Extension(Extension::IntervalMDM),_ => return Err(invalid()),
            },
            "map" => Self::map(arg(args()?,0),scope)?,_ => return Err(invalid()),
        })
    }
    pub(super) fn map(options: &Object,scope: &EvaluationScope) -> Result<Self> {
        charge_clone(options,scope)?;let options = options.clone().destructure_map()?;
        charge_clone(&options,scope)?;let sorted = options.lookup_keyword("sorted?")?;
        Ok(Self::Map(sorted.truthy()))
    }
    pub(super) fn representation(&self,scope: &EvaluationScope) -> Result<Object> {
        let kw = |name|keyword(name,scope);let int = |value|integer(value,scope);
        match self {
            Self::Bool => kw("bool"),Self::Int { width,signed: true } => kw(match width { 8 => "i8",16 => "i16",32 => "i32",64 => "i64",_ => return Err(invalid()) }),Self::Int { signed: false,.. } => Err(invalid()),
            Self::Floating(double) => kw(if *double { "f64" } else { "f32" }),Self::Utf8 => kw("utf8"),Self::Binary => kw("varbinary"),
            Self::FixedSizeBinary(width) => vector([kw("fixed-size-binary")?,int(*width)?],scope),Self::FixedSizeList(width) => vector([kw("fixed-size-list")?,int(*width)?],scope),
            Self::Decimal { precision,scale,width } => vector([kw("decimal")?,int(*precision)?,int(*scale)?,int(*width)?],scope),
            Self::Date(milli) => vector([kw("date")?,kw(if *milli { "milli" } else { "day" })?],scope),
            Self::Time { unit,.. } => vector([kw("time-local")?,kw(unit.keyword())?],scope),Self::Duration(unit) => vector([kw("duration")?,kw(unit.keyword())?],scope),
            Self::Timestamp { unit: Unit::Micro,timezone: Some(timezone) } if timezone.as_ref() == "UTC" => kw("instant"),
            Self::Timestamp { unit,timezone: Some(timezone) } => { scope.reserve(timezone.len())?;vector([kw("timestamp-tz")?,kw(unit.keyword())?,Object::String(timezone.to_string())],scope) }
            Self::Timestamp { unit,timezone: None } => vector([kw("timestamp-local")?,kw(unit.keyword())?],scope),
            Self::Interval(unit) => vector([kw("interval")?,kw(unit.keyword())?],scope),
            Self::Struct => kw("struct"),Self::List => kw("list"),Self::Union(dense) => kw(if *dense { "union" } else { "sparse-union" }),
            Self::Map(sorted) => { let mut options = Map::new();assoc(&mut options,kw("sorted?")?,Object::Bool(*sorted),scope)?;vector([kw("map")?,Object::Map(options)],scope) }
            Self::Extension(Extension::IntervalMDM) => vector([kw("interval")?,kw("month-day-micro")?],scope),
            Self::Extension(extension) => kw(extension.keyword()),
        }
    }
    pub(super) fn java_hash(&self) -> i32 {
        let fields = |values: &[i32]|values.iter().fold(1i32,|hash,value|combine(hash,*value));
        match self {
            Self::Bool | Self::Utf8 | Self::Binary | Self::Struct | Self::List => 1,
            Self::Int { width,signed } => fields(&[*width,if *signed { 1231 } else { 1237 }]),
            Self::Floating(double) => fields(&[enum_hash("org.apache.arrow.vector.types.FloatingPointPrecision",if *double { "DOUBLE" } else { "SINGLE" })]),
            Self::FixedSizeBinary(width) | Self::FixedSizeList(width) => fields(&[*width]),Self::Decimal { precision,scale,width } => fields(&[*precision,*scale,*width]),
            Self::Date(milli) => fields(&[enum_hash("org.apache.arrow.vector.types.DateUnit",if *milli { "MILLISECOND" } else { "DAY" })]),
            Self::Time { unit,width } => fields(&[unit.java_hash(),*width]),Self::Timestamp { unit,timezone } => fields(&[unit.java_hash(),timezone.as_deref().map_or(0,hash::string)]),
            Self::Duration(unit) => fields(&[unit.java_hash()]),Self::Interval(unit) => fields(&[enum_hash("org.apache.arrow.vector.types.IntervalUnit",unit.name())]),
            Self::Map(sorted) => fields(&[if *sorted { 1231 } else { 1237 }]),Self::Union(dense) => fields(&[enum_hash("org.apache.arrow.vector.types.UnionMode",if *dense { "Dense" } else { "Sparse" }),0]),
            Self::Extension(extension) => fields(&[extension.storage().java_hash(),hash::string(extension.name())]),
        }
    }
    pub(super) fn write_string(&self,text: &mut Text<'_>) -> Result<()> {
        let number = |text: &mut Text<'_>,value: i32|text.temporary(32,||Ok(value.to_string()));
        match self {
            Self::Bool => text.push("Bool"),Self::Utf8 => text.push("Utf8"),Self::Binary => text.push("Binary"),Self::Struct => text.push("Struct"),Self::List => text.push("List"),
            Self::Int { width,signed } => { text.push("Int(")?;number(text,*width)?;text.push(if *signed { ", true)" } else { ", false)" }) }
            Self::Floating(double) => text.push(if *double { "FloatingPoint(DOUBLE)" } else { "FloatingPoint(SINGLE)" }),
            Self::FixedSizeBinary(width) | Self::FixedSizeList(width) => { text.push(if matches!(self,Self::FixedSizeBinary(_)) { "FixedSizeBinary(" } else { "FixedSizeList(" })?;number(text,*width)?;text.push(")") }
            Self::Decimal { precision,scale,width } => { text.push("Decimal(")?;number(text,*precision)?;text.push(", ")?;number(text,*scale)?;text.push(", ")?;number(text,*width)?;text.push(")") }
            Self::Date(milli) => text.push(if *milli { "Date(MILLISECOND)" } else { "Date(DAY)" }),
            Self::Time { unit,width } => { text.push("Time(")?;text.push(unit.name())?;text.push(", ")?;number(text,*width)?;text.push(")") }
            Self::Timestamp { unit,timezone } => { text.push("Timestamp(")?;text.push(unit.name())?;text.push(", ")?;text.push(timezone.as_deref().unwrap_or("null"))?;text.push(")") }
            Self::Duration(unit) => { text.push("Duration(")?;text.push(unit.name())?;text.push(")") }
            Self::Interval(unit) => { text.push("Interval(")?;text.push(unit.name())?;text.push(")") }
            Self::Map(sorted) => text.push(if *sorted { "Map(true)" } else { "Map(false)" }),Self::Union(dense) => text.push(if *dense { "Union(Dense, null)" } else { "Union(Sparse, null)" }),
            Self::Extension(extension) => { text.push("ExtensionType(")?;text.push(extension.name())?;text.push(", ")?;extension.storage().write_string(text)?;text.push(")") }
        }
    }
}
impl Unit {
    fn read(value: &Object) -> Result<Self> { Ok(match key(value)? { "second" => Self::Second,"milli" => Self::Milli,"micro" => Self::Micro,"nano" => Self::Nano,_ => return Err(invalid()) }) }
    fn keyword(self) -> &'static str { match self { Self::Second => "second",Self::Milli => "milli",Self::Micro => "micro",Self::Nano => "nano" } }
    fn name(self) -> &'static str { match self { Self::Second => "SECOND",Self::Milli => "MILLISECOND",Self::Micro => "MICROSECOND",Self::Nano => "NANOSECOND" } }
    fn java_hash(self) -> i32 { enum_hash("org.apache.arrow.vector.types.TimeUnit",self.name()) }
}
impl Interval {
    fn keyword(self) -> &'static str { match self { Self::YearMonth => "year-month",Self::DayTime => "day-time",Self::MonthDayNano => "month-day-nano" } }
    fn name(self) -> &'static str { match self { Self::YearMonth => "YEAR_MONTH",Self::DayTime => "DAY_TIME",Self::MonthDayNano => "MONTH_DAY_NANO" } }
}
impl Extension {
    fn keyword(self) -> &'static str { match self { Self::Keyword => "keyword",Self::Transit => "transit",Self::Set => "set",Self::TsTzRange => "tstz-range",Self::IntervalMDM => "interval",Self::Uuid => "uuid",Self::Uri => "uri",Self::Oid => "oid",Self::RegClass => "regclass",Self::RegProc => "regproc" } }
    fn name(self) -> &'static str { match self { Self::Keyword => "xt/clj-keyword",Self::Transit => "xt/transit+msgpack",Self::Set => "set",Self::TsTzRange => "xt/tstz-range",Self::IntervalMDM => "xt/intervalDMMType",Self::Uuid => "uuid",Self::Uri => "uri",Self::Oid => "xt/oid",Self::RegClass => "xt/regclass",Self::RegProc => "xt/regproc" } }
    fn storage(self) -> ArrowKind { match self { Self::Keyword | Self::Uri => ArrowKind::Utf8,Self::Transit => ArrowKind::Binary,Self::Set => ArrowKind::List,Self::TsTzRange => ArrowKind::FixedSizeList(2),Self::IntervalMDM => ArrowKind::Interval(Interval::MonthDayNano),Self::Uuid => ArrowKind::FixedSizeBinary(16),Self::Oid | Self::RegClass | Self::RegProc => ArrowKind::Int { width: 32,signed: true } } }
}
fn key(value: &Object) -> Result<&str> { match value { Object::Keyword(value) => Ok(value),_ => Err(invalid()) } }
fn arg(args: &[Object],index: usize) -> &Object { args.get(index).unwrap_or(&Object::Null) }
fn int(value: &Object,character: bool,scope: &EvaluationScope) -> Result<i32> {
    if !character && !matches!(value,Object::Number(_)) { return Err(invalid()); }
    if let Object::Number(value) = value { scope.reserve(value.text_capacity().map_err(super::super::number::error)?.checked_mul(16).ok_or_else(overflow)?)?; }
    match super::super::number::cast("i32",value)? { super::super::Number::Int(value) => Ok(value),_ => unreachable!() }
}
fn charge_clone(value: &Object,scope: &EvaluationScope) -> Result<()> {
    match value {
        Object::String(value) | Object::Keyword(value) | Object::Symbol(value) | Object::Uri(value) | Object::KeyFn(value) => scope.reserve(value.len().checked_mul(3).ok_or_else(overflow)?),
        Object::Vector(values) | Object::List(values) => { slots(values.len(),scope)?;for value in values { charge_clone(value,scope)?; }Ok(()) }
        Object::Map(map) | Object::Set(map) => { slots(map.len(),scope)?;for (key,value) in map.entries() { charge_clone(key,scope)?;charge_clone(value,scope)?; }Ok(()) }
        Object::TxKey(record) => { slots(record.entries().count(),scope)?;for (key,value) in record.entries() { charge_clone(key,scope)?;charge_clone(value,scope)?; }Ok(()) }
        Object::ClojureForm(value) => { slots(1,scope)?;charge_clone(value,scope) }
        Object::Path(value) => scope.reserve(super::super::allocation::path_capacity(value)?.checked_mul(3).ok_or_else(overflow)?),Object::Time(_) => scope.reserve(4096),_ => Ok(()),
    }
}
