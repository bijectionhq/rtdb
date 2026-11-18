use rtdb_types::{Anomaly, Result as NativeResult, java_text::{JavaString, JavaText}};
use super::Mode;
use super::super::response::{Metadata, Properties, Rule, RuleKind, RuleMode, RuleSet};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ListDemand { Membership, ReadCopy, DowngradeCopy, UpgradeSize }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Unchecked {
    MissingSource,
    NullList(ListDemand),
    NullRule,
    NullHeaderValue,
    NullRuleName,
    NullExecutorType,
    NullKind,
    ActionIndex(usize),
}

#[derive(Debug)]
pub(super) enum Failure { Native(Anomaly), Unchecked(Unchecked) }
impl From<Anomaly> for Failure { fn from(cause: Anomaly) -> Self { Self::Native(cause) } }
impl From<Unchecked> for Failure { fn from(cause: Unchecked) -> Self { Self::Unchecked(cause) } }
type Result<T> = std::result::Result<T, Failure>;

#[derive(Clone, Copy)]
pub(super) struct RuleSchema<'a> { rules: Option<&'a RuleSet>, metadata: Option<&'a Metadata> }
impl<'a> RuleSchema<'a> {
    pub(super) fn new(rules: Option<&'a RuleSet>, metadata: Option<&'a Metadata>) -> Self { Self { rules, metadata } }
}

#[derive(Clone, Copy)]
pub(super) struct RuleSetView<'a>(&'a RuleSet);
impl<'a> RuleSetView<'a> {
    pub(super) fn new(rules: &'a RuleSet) -> Self { Self(rules) }
    pub(super) fn has_rules(self, mode: RuleMode) -> Result<bool> {
        let entries = match mode {
            RuleMode::Upgrade | RuleMode::Downgrade | RuleMode::UpDown => &self.0.migration_rules,
            RuleMode::Write | RuleMode::Read | RuleMode::WriteRead => &self.0.domain_rules,
        }.as_ref().ok_or(Unchecked::NullList(ListDemand::Membership))?;
        for entry in entries.as_slice() {
            let rule = entry.as_ref().ok_or(Unchecked::NullRule)?;
            let matches = rule.mode == Some(mode) || matches!((mode, rule.mode),
                (RuleMode::Upgrade | RuleMode::Downgrade, Some(RuleMode::UpDown)) |
                (RuleMode::Write | RuleMode::Read, Some(RuleMode::WriteRead)));
            if matches { return Ok(true); }
        }
        Ok(false)
    }
}

#[derive(Clone, Copy)]
pub(super) struct MetadataView<'a>(&'a Metadata);
impl<'a> MetadataView<'a> {
    pub(super) fn new(metadata: &'a Metadata) -> Self { Self(metadata) }
    pub(super) fn property(self, name: JavaText<'_>) -> Option<JavaText<'a>> { property(self.0.properties.as_ref(), name) }
}

#[derive(Clone, Copy)]
pub(super) enum FailedRuleHeader<'a> { Absent, NullValue, Name(JavaText<'a>) }
pub(super) trait RuleDecisions {
    fn disabled(&mut self, rule: RuleView<'_>) -> NativeResult<bool>;
    fn failed_rule_header(&mut self) -> NativeResult<FailedRuleHeader<'_>>;
}

pub(super) struct RuleWalk<'a> {
    rules: &'a [Option<Rule>], position: usize, mode: Mode, metadata: Option<MetadataView<'a>>,
}
pub(super) enum RuleStep<'a> { Done, Rule { rule: RuleView<'a>, remaining: RuleWalk<'a> } }
impl<'a> RuleWalk<'a> {
    pub(super) fn start(mode: Mode, message_is_null: bool, source: Option<RuleSchema<'a>>, target: Option<RuleSchema<'a>>) -> Result<Self> {
        if message_is_null || target.is_none() { return Ok(Self { rules: &[], position: 0, mode, metadata: None }); }
        let target = target.unwrap();
        let (selected, demand) = match mode {
            Mode::Read => (target.rules.map(|rules| &rules.domain_rules), ListDemand::ReadCopy),
            Mode::Upgrade => (target.rules.map(|rules| &rules.migration_rules), ListDemand::UpgradeSize),
            Mode::Downgrade => (source.ok_or(Unchecked::MissingSource)?.rules.map(|rules| &rules.migration_rules), ListDemand::DowngradeCopy),
        };
        let rules: &'a [Option<Rule>] = match selected {
            None => &[],
            Some(entries) => entries.as_ref().ok_or(Unchecked::NullList(demand))?.as_slice(),
        };
        Ok(Self { rules, position: 0, mode, metadata: target.metadata.map(MetadataView::new) })
    }

    pub(super) fn next(mut self, decisions: &mut impl RuleDecisions) -> Result<RuleStep<'a>> {
        while self.position < self.rules.len() {
            let index = self.position;
            let selected = if self.mode == Mode::Upgrade { index } else { self.rules.len() - index - 1 };
            self.position += 1;
            let rule = self.rules[selected].as_ref().ok_or(Unchecked::NullRule)?;
            let rule = RuleView { rule, mode: self.mode, index, metadata: self.metadata };
            if decisions.disabled(rule)? { continue; }
            match decisions.failed_rule_header()? {
                FailedRuleHeader::Absent => (),
                FailedRuleHeader::NullValue => return Err(Unchecked::NullHeaderValue.into()),
                FailedRuleHeader::Name(name) => {
                    if same(rule.name().ok_or(Unchecked::NullRuleName)?, name) { continue; }
                }
            }
            if !accepts(self.mode, rule.rule.mode) { continue; }
            return Ok(RuleStep::Rule { rule, remaining: self });
        }
        Ok(RuleStep::Done)
    }
}

#[derive(Clone, Copy)]
pub(super) struct RuleView<'a> { rule: &'a Rule, mode: Mode, index: usize, metadata: Option<MetadataView<'a>> }
impl<'a> RuleView<'a> {
    pub(super) fn name(self) -> Option<JavaText<'a>> { self.rule.name.as_ref().map(JavaString::view) }
    pub(super) fn mode(self) -> Option<RuleMode> { self.rule.mode }
    pub(super) fn index(self) -> usize { self.index }
    pub(super) fn executor_type(self) -> Result<JavaText<'a>> {
        self.rule.rule_type.as_ref().map(JavaString::view).ok_or_else(|| Unchecked::NullExecutorType.into())
    }
    pub(super) fn kind_after_transform(self) -> Result<RuleKind> { self.rule.kind.ok_or_else(|| Unchecked::NullKind.into()) }
    pub(super) fn on_success(self) -> Option<JavaText<'a>> { self.rule.on_success.as_ref().map(JavaString::view) }
    pub(super) fn on_failure(self) -> Option<JavaText<'a>> { self.rule.on_failure.as_ref().map(JavaString::view) }
    pub(super) fn parameter(self, name: JavaText<'_>) -> Option<JavaText<'a>> {
        property(self.rule.params.as_ref(), name).or_else(|| self.metadata.and_then(|metadata| metadata.property(name)))
    }
    pub(super) fn action_name<'b>(self, action: Option<JavaText<'b>>) -> Result<Option<JavaText<'b>>> {
        let Some(action) = action else { return Ok(None); };
        if !matches!(self.rule.mode, Some(RuleMode::WriteRead | RuleMode::UpDown)) || !action.units().any(|unit| unit == 44) {
            return Ok(Some(action));
        }
        let index = usize::from(self.mode != Mode::Upgrade);
        let end = action.units().enumerate().filter_map(|(index, unit)| (unit != 44).then_some(index + 1)).last().unwrap_or(0);
        if end == 0 { return Err(Unchecked::ActionIndex(index).into()); }
        let trimmed = action.slice_units(0..end).unwrap();
        let first_end = trimmed.units().position(|unit| unit == 44).unwrap_or(end);
        if index == 0 { return Ok(trimmed.slice_units(0..first_end)); }
        if first_end == end { return Err(Unchecked::ActionIndex(index).into()); }
        let start = first_end + 1;
        let rest = trimmed.slice_units(start..end).unwrap();
        let second_end = rest.units().position(|unit| unit == 44).unwrap_or(rest.len_units());
        Ok(rest.slice_units(0..second_end))
    }
}

fn accepts(execution: Mode, mode: Option<RuleMode>) -> bool {
    matches!((execution, mode),
        (Mode::Read, Some(RuleMode::Read | RuleMode::WriteRead)) |
        (Mode::Upgrade, Some(RuleMode::Upgrade | RuleMode::UpDown)) |
        (Mode::Downgrade, Some(RuleMode::Downgrade | RuleMode::UpDown)))
}
fn same(left: JavaText<'_>, right: JavaText<'_>) -> bool { left.units().eq(right.units()) }
fn property<'a>(properties: Option<&'a Properties>, name: JavaText<'_>) -> Option<JavaText<'a>> {
    properties?.values.as_slice().iter().find(|(key, _)| same(key.view(), name))?.1.as_ref().map(JavaString::view)
}

#[cfg(test)]
mod tests;
