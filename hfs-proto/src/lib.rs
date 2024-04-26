mod gen {
    include!(concat!(env!("OUT_DIR"), "/proto_gen.rs"));
}

pub use gen::proto_gen::*;
