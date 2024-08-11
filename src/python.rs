use crate::stream::{LineStream, StreamError, from_file, from_http};
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::Path;
use url::Url;

impl From<StreamError> for PyErr {
    fn from(err: StreamError) -> Self {
        match err {
            StreamError::Url(err) => PyValueError::new_err(err.to_string()),
            StreamError::Http(io_err) => PyIOError::new_err(io_err.to_string()),
            StreamError::Io(io_err) => PyIOError::new_err(io_err.to_string()),
        }
    }
}

#[pyclass]
struct LineIterator {
    iter: LineStream,
}

#[pymethods]
impl LineIterator {
    #[new]
    fn new(source: &str) -> PyResult<Self> {
        Ok(Self {
            iter: if source.starts_with("http") {
                from_http(&Url::parse(&source).map_err(|e| PyValueError::new_err(e.to_string()))?)?
            } else {
                from_file(Path::new(&source))?
            },
        })
    }

    fn __iter__(slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<String>> {
        match self.iter.next() {
            Some(Ok(line)) => Ok(Some(line)),
            Some(Err(err)) => Err(PyIOError::new_err(err.to_string())),
            None => Ok(None),
        }
    }
}

#[pyfunction]
fn stream_lines(source: &str) -> PyResult<LineIterator> {
    LineIterator::new(&source)
}

#[pymodule]
fn pvvortex(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(stream_lines, m)?)?;
    Ok(())
}
