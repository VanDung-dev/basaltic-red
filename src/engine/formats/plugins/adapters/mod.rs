pub mod avro;
pub mod excel;
pub mod msgpack;
pub mod orc;

pub(crate) use avro::inspect_avro_blocks;
pub use avro::{read_avro_range, AvroHandler};
pub use excel::XlsxHandler;
pub(crate) use msgpack::inspect_msgpack_blocks;
pub use msgpack::{read_msgpack_range, MsgpackHandler};
pub use orc::{read_orc_range, OrcHandler};
