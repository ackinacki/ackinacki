use async_graphql::Enum;

#[allow(clippy::upper_case_acronyms)]
#[derive(Enum, Copy, Clone, Eq, PartialEq)]
/// Due to GraphQL limitations big numbers are returned as a string. You can
/// specify format used to string representation for big integers.
pub enum BigIntFormat {
    /// Hexadecimal representation started with 0x (default)
    HEX,
    /// Decimal representation
    DEC,
}
