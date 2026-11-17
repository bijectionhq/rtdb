use super::{Mapping, big5::Big5Family};
mod aliases;
mod tables;

struct Overlay(&'static [u8]);

pub(super) enum Base<'a> {
    Big5(&'a Big5Family),
    Ms950(&'a Mapping),
}

pub(in super::super::super) struct Hkscs {
    overlay: &'static Overlay,
    base: Base<'static>,
}

pub(in super::super) fn lookup(name: &str) -> Option<&'static Hkscs> {
    let index = aliases::ALIASES.binary_search_by_key(&name, |&(alias, _)| alias).ok()?;
    Some(&tables::TABLES[aliases::ALIASES[index].1])
}

impl Overlay {
    fn pair(&self, first: u8, second: u8) -> u32 {
        let offset = usize::from(first) * 2;
        let row = u16::from_be_bytes([self.0[offset], self.0[offset + 1]]);
        if row == 0xffff { return 0xfffd; }
        let offset = 512 + (usize::from(row) * 191 + usize::from(second - 0x40)) * 4;
        u32::from_be_bytes(self.0[offset..offset + 4].try_into().unwrap())
    }
}

impl Base<'_> {
    fn single(&self, byte: u8) -> u16 {
        match self {
            Self::Big5(family) => family.single(byte),
            Self::Ms950(mapping) => mapping.single(byte),
        }
    }

    pub(super) fn pair(&self, first: u8, second: u8) -> u16 {
        match self {
            Self::Big5(family) => family.pair(first, second),
            Self::Ms950(mapping) => mapping.pair(first, second),
        }
    }
}

impl Hkscs {
    pub(in super::super) fn decode(&self, mut bytes: &[u8]) -> String {
        let mut output = String::with_capacity(bytes.len() * 3);
        while let Some((&first, tail)) = bytes.split_first() {
            bytes = tail;
            let mut scalar = u32::from(self.base.single(first));
            if scalar == 0xfffd {
                if let Some((&second, tail)) = bytes.split_first() {
                    // OpenJDK25 HKSCS.Decoder's ArrayDecoder path consumes both bytes for every complete unmappable pair.
                    bytes = tail;
                    if (0x40..=0xfe).contains(&second) {
                        scalar = self.overlay.pair(first, second);
                        if scalar == 0xfffd { scalar = u32::from(self.base.pair(first, second)); }
                    }
                }
            }
            output.push(char::from_u32(scalar).unwrap_or('\u{fffd}'));
        }
        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_names_share_overlays_and_borrow_their_base_owner() {
        assert_eq!(tables::TABLES.len(), 4);
        assert_eq!(tables::OVERLAYS.len(), 3);
        assert_eq!(aliases::ALIASES.len(), 14);
        for pair in aliases::ALIASES.windows(2) { assert!(pair[0].0 < pair[1].0); }
        for &(name, index) in aliases::ALIASES {
            assert!(std::ptr::eq(lookup(name).unwrap(), &tables::TABLES[index]));
        }
        assert!(std::ptr::eq(tables::TABLES[0].overlay, tables::TABLES[2].overlay));
        for (index, table) in tables::TABLES.iter().enumerate() {
            match &table.base {
                Base::Big5(family) => {
                    assert!(index < 2);
                    assert!(std::ptr::eq(*family, &super::super::big5::FAMILY));
                }
                Base::Ms950(mapping) => {
                    assert!(index >= 2);
                    assert!(std::ptr::eq(*mapping, super::super::lookup("x-windows-950").unwrap()));
                }
            }
            for byte in 0u8..=255 { assert_eq!(table.base.single(byte), if byte < 128 { u16::from(byte) } else { 0xfffd }); }
            for (second, scalar) in (0xd6..=0xdc).zip("碁銹裏墻恒粧嫺".chars()) {
                assert_eq!(table.overlay.pair(0xf9, second), u32::from(scalar));
            }
            assert_eq!(table.overlay.pair(0xa4, 0xa4), 0xfffd);
            assert_eq!(table.base.pair(0xa4, 0xa4), 0x4e2d);
        }
    }

    #[test]
    fn compact_overlays_contain_valid_bmp_and_supplementary_scalars() {
        for (index, overlay) in tables::OVERLAYS.iter().enumerate() {
            assert_eq!((overlay.0.len() - 512) % (191 * 4), 0);
            let rows = (overlay.0.len() - 512) / (191 * 4);
            for pair in overlay.0[..512].chunks_exact(2) {
                let row = u16::from_be_bytes([pair[0], pair[1]]);
                assert!(row == 0xffff || usize::from(row) < rows);
            }
            let mut supplementary = 0;
            for bytes in overlay.0[512..].chunks_exact(4) {
                let scalar = u32::from_be_bytes(bytes.try_into().unwrap());
                assert!(char::from_u32(scalar).is_some());
                assert!(scalar <= 0xffff || (0x20000..=0x2ffff).contains(&scalar));
                supplementary += usize::from(scalar > 0xffff);
            }
            assert_eq!(supplementary > 0, index < 2);
        }
    }
}
