use pyo3::prelude::*;

pub mod engine;
pub mod error;
pub mod filter;
pub mod pyapi;
pub mod utils;

#[pymodule]
mod basaltic_red {
    #[pymodule_export]
    use crate::engine::{MatrixEngine, PyBatchIterator};

    #[pymodule_export]
    use crate::pyapi::dictionary::dictionary;
    #[pymodule_export]
    use crate::pyapi::filter::filter;
    #[pymodule_export]
    use crate::pyapi::formats::formats;
    #[pymodule_export]
    use crate::pyapi::graph::graph;
    #[pymodule_export]
    use crate::pyapi::lake::lake;
    #[pymodule_export]
    use crate::pyapi::read::read;
    #[pymodule_export]
    use crate::pyapi::sql::sql;
}
