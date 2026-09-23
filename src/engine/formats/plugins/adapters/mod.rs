pub mod avro;
pub mod excel;
pub mod msgpack;
pub mod orc;

pub(crate) use avro::inspect_avro_blocks;
pub use avro::{read_avro_range, AvroHandler};
pub use excel::XlsxHandler;
pub use msgpack::MsgpackHandler;
pub use orc::{read_orc_range, OrcHandler};
