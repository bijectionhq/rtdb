#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct DecodedUnit {
    pub(super) unit: u16,
    pub(super) bytes: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum DecodeError {
    Malformed { bytes: usize },
    Unmappable { bytes: usize },
}

impl DecodeError {
    pub(super) fn bytes(self) -> usize {
        match self { Self::Malformed { bytes } | Self::Unmappable { bytes } => bytes }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ProbeStop { Underflow, Error(DecodeError) }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct StrictProbe {
    pub(super) consumed: usize,
    pub(super) utf16_units: usize,
    pub(super) stop: ProbeStop,
}

#[cfg(test)]
mod tests;
