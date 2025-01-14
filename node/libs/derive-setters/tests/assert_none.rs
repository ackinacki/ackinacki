use derive_setters::*;

#[derive(Default, Setters, Debug, PartialEq, Eq)]
#[setters(strip_option, assert_none)]
struct StripAssertNoneStruct {
    a: Option<u32>,
    #[setters(strip_option = false)]
    c: std::option::Option<u32>,
    d: u32,
}

#[test]
fn assert_none_must_not_panic_for_the_first_set() {
    assert_eq!(
        StripAssertNoneStruct::default().a(3).c(Some(42)).d(7),
        StripAssertNoneStruct { a: Some(3), c: Some(42), d: 7 },
    );
}

#[test]
#[should_panic]
fn assert_none_panic_if_value_was_set() {
    let x = StripAssertNoneStruct::default().c(Some(3));
    x.c(Some(3));
}

#[test]
#[should_panic]
fn assert_none_panic_if_value_was_set_for_stripped_option() {
    let x = StripAssertNoneStruct::default().a(1);
    x.a(1);
}
