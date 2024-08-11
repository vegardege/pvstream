use crate::parse::{DomainCode, PageviewsRow};
use crate::stream::StreamError;
use crate::{RowError, RowIterator, parse_lines_from_file, parse_lines_from_http};
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::Path;
use url::Url;

impl From<RowError> for PyErr {
    fn from(err: RowError) -> Self {
        match err {
            RowError::Io(io_err) => PyIOError::new_err(io_err.to_string()),
            RowError::Parse(parse_err) => PyValueError::new_err(parse_err.to_string()),
        }
    }
}

impl From<StreamError> for PyErr {
    fn from(err: StreamError) -> Self {
        match err {
            StreamError::Url(url_err) => PyValueError::new_err(url_err.to_string()),
            StreamError::Http(io_err) => PyIOError::new_err(io_err.to_string()),
            StreamError::Io(io_err) => PyIOError::new_err(io_err.to_string()),
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PyDomainCode {
    #[pyo3(get)]
    pub language: String,
    #[pyo3(get)]
    pub domain: Option<String>,
    #[pyo3(get)]
    pub mobile: bool,
}

impl From<DomainCode> for PyDomainCode {
    fn from(inner: DomainCode) -> Self {
        Self {
            language: inner.language,
            domain: inner.domain.map(str::to_string),
            mobile: inner.mobile,
        }
    }
}

#[pyclass]
pub struct PyPageviewsRow {
    #[pyo3(get)]
    pub domain_code: String,
    #[pyo3(get)]
    pub page_title: String,
    #[pyo3(get)]
    pub views: u32,
    #[pyo3(get)]
    pub parsed_domain_code: PyDomainCode,
}

impl From<PageviewsRow> for PyPageviewsRow {
    fn from(inner: PageviewsRow) -> Self {
        Self {
            domain_code: inner.domain_code,
            page_title: inner.page_title,
            views: inner.views,
            parsed_domain_code: inner.parsed_domain_code.into(),
        }
    }
}

#[pyclass]
struct PyRowIterator {
    iter: RowIterator,
}

#[pymethods]
impl PyRowIterator {
    #[new]
    fn new(source: &str) -> PyResult<Self> {
        let iter = if source.starts_with("http") {
            let url = Url::parse(source).map_err(|e| PyValueError::new_err(e.to_string()))?;
            parse_lines_from_http(url)?
        } else {
            let path = Path::new(source);
            parse_lines_from_file(path)?
        };

        Ok(Self { iter })
    }

    fn __iter__(slf: PyRefMut<Self>) -> PyRefMut<Self> {
        slf
    }

    fn __next__(&mut self) -> PyResult<Option<PyPageviewsRow>> {
        match self.iter.next() {
            Some(Ok(row)) => Ok(Some(row.into())),
            Some(Err(err)) => Err(err.into()),
            None => Ok(None),
        }
    }
}

#[pyfunction]
fn stream_lines(source: &str) -> PyResult<PyRowIterator> {
    PyRowIterator::new(&source)
}

#[pymodule]
fn pvvortex(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyPageviewsRow>()?;
    m.add_class::<PyDomainCode>()?;
    m.add_function(wrap_pyfunction!(stream_lines, m)?)?;
    Ok(())
}
