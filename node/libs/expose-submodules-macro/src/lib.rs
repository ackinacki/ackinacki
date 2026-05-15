#[macro_export]
macro_rules! submodule {
    ( $( $x:ident ),* ) => {
        $(
            mod $x;
            pub use self::$x::*;
        )*
    };
}
