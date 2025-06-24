use std::fmt::Debug;

#[allow(dead_code)]
pub(crate) fn detailed(err: &impl Debug) -> String {
    repl(
        format!("{:#?}", err),
        [
            ("\n", " "),
            ("  ", " "),
            ("( ", "("),
            (" )", ")"),
            ("{ ", "{"),
            (" }", "}"),
            (",)", ")"),
            (",}", "}"),
        ],
    )
}

pub(crate) fn repl<const N: usize>(mut s: String, old_new: [(&str, &str); N]) -> String {
    for (old, new) in old_new {
        while s.contains(old) {
            s = s.replace(old, new);
        }
    }
    s
}
