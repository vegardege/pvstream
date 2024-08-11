use crate::parse::{DomainCode, PageviewsRow};
use crate::{RowError, RowIterator, stream_from_file, stream_from_http};
use pyo3::exceptions::PyIOError;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::path::Path;
use std::sync::Mutex;
use url::Url;

impl From<RowError> for PyErr {
    fn from(err: RowError) -> Self {
        match err {
            RowError::Io(io_err) => PyIOError::new_err(io_err.to_string()),
            RowError::Parse(parse_err) => PyValueError::new_err(parse_err.to_string()),
        }
    }
}

#[pyclass(name = "DomainCode")]
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

#[pymethods]
impl PyDomainCode {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "DomainCode(language={:?}, domain={:?}, mobile={})",
            self.language, self.domain, self.mobile
        ))
    }
}

#[pyclass(name = "PageviewsRow")]
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

#[pymethods]
impl PyPageviewsRow {
    fn __repr__(&self) -> PyResult<String> {
        Ok(format!(
            "PageviewsRow(domain_code={:?}, page_title={:?}, views={})",
            self.domain_code, self.page_title, self.views
        ))
    }
}

#[pyclass(name = "RowIterator")]
struct PyRowIterator {
    iter: Mutex<RowIterator>,
}

#[pymethods]
impl PyRowIterator {
    #[new]
    fn new(source: &str) -> PyResult<Self> {
        let iter = if source.starts_with("http") {
            let url = Url::parse(source).map_err(|e| PyValueError::new_err(e.to_string()))?;
            stream_from_http(url)?
        } else {
            let path = Path::new(source);
            stream_from_file(path)?
        };

        Ok(Self {
            iter: Mutex::new(iter),
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(slf: PyRefMut<'_, Self>) -> PyResult<Option<PyPageviewsRow>> {
        match slf.iter.lock().unwrap().next() {
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
