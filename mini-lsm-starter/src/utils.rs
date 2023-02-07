pub trait FromLeBytesSlice {
    fn from_le_bytes_slice(bytes: &[u8]) -> u16;
}

impl FromLeBytesSlice for u16 {
    fn from_le_bytes_slice(bytes: &[u8]) -> u16 {
        u16::from_le_bytes(bytes.try_into().unwrap())
    }
}
