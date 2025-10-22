# pvstream

[![Code Quality and Tests](https://github.com/vegardege/pvstream/actions/workflows/code-quality.yml/badge.svg)](https://github.com/vegardege/pvstream/actions/workflows/code-quality.yml)
[![PyPI](https://img.shields.io/pypi/v/pvstream)](https://pypi.org/project/pvstream/)
[![Crates.io Version](https://img.shields.io/crates/v/pvstream)](https://crates.io/crates/pvstream/)
[![docs.rs](https://img.shields.io/docsrs/pvstream)](https://docs.rs/pvstream)

`pvstream` is a Rust library with python bindings allowing you to efficiently
stream download, parse, and filter pageviews from Wikimedia's hourly dumps.

The library can be used from Rust or python. In both languages you can choose
between an iterator of parsed objects, made available on the fly as the file
is downloaded, or a complete parquet file of parsed and filtered data.

## Installation

### Rust

Add `pvstream` to your `Cargo.toml`:

```toml
[dependencies]
pvstream = "0.1.0-alpha.1"
```

Or use cargo-add:

```bash
cargo add pvstream
```

### Python

Install from PyPI:

```bash
pip install pvstream
```

### Building from Source

To build the Python package for your specific hardware:

```bash
pip install maturin
git clone https://github.com/vegardege/pvstream
cd pvstream
maturin develop --release
```

Or build a wheel:

```bash
maturin build --release
pip install target/wheels/pvstream-*.whl
```

## Usage

There are four main entry points for this library:

| Function            |  Input                            |  Output                            |
| ------------------- | --------------------------------- | ---------------------------------- |
| `stream_from_file`  | Filename on the local file system | Iterator of parsed row structs     |
| `stream_from_url`   | URL of a remotely stored file     | Iterator of parsed row structs     |
| `parquet_from_file` | Filename on the local file system | Parquet file of parsed row structs |
| `parquet_from_url`  | URL of a remotely stored file     | Parquet file of parsed row structs |

> [!CAUTION]
> The `_url` functions will stream the file directly from Wikimedia's servers.
> Please be kind to the servers and cache if you plan to read the same file
> more than once. Consider using a mirror closer to you. You can find
> mirrors listed on [wikimedia.org](https://dumps.wikimedia.org/mirrors.html).

They all accept similar filters. In python, `Regex` is a `str`, `Vec` is a `list`, `u32` is an `int`:

| Filter         | Type                  | Description                                                 |
| -------------- | --------------------- | ----------------------------------------------------------- |
| `line_regex`   | `Option<Regex>`       | Regular expression used to filter lines before parsing      |
| `page_title`   | `Option<Regex>`       | Regular expression used to filter page titles after parsing |
| `domain_codes` | `Option<Vec<String>>` | List of domain codes to accept                              |
| `min_views`    | `Option<u32>`         | Minimum amount of views needed to be accepted               |
| `max_views`    | `Option<u32>`         | Maximum amount of views allowed                             |
| `languages`    | `Option<Vec<String>>` | List of languages to accept                                 |
| `domains`      | `Option<Vec<String>>` | List of domains to accept                                   |
| `mobile`       | `Option<bool>`        | If set, filter on whether the row belongs to a mobile site  |

Learn more about the format from [Wikimedia's documentation](https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews).

#### Example (Rust):

```rust
use pvstream::filter::FilterBuilder;
use pvstream::stream_from_file;
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Path to your pageviews file
    let path = PathBuf::from("pageviews-20240818-080000.gz");

    // View all English mobile sites containing the word 'Rust'
    let filter = FilterBuilder::new()
        .domain_codes(["en.m"])
        .page_title("Rust")
        .build();

    // Stream rows matching the filter
    let rows = stream_from_file(path, &filter)?;

    // Iterate over results
    for row in rows {
        match row {
            Ok(pageview) => println!("{:?}", pageview),
            Err(e) => eprintln!("Error parsing row: {:?}", e),
        }
    }

    Ok(())
}
```

#### Example (python):

```python
import pvstream

rows = pvstream.stream_from_file(
    "pageviews-20240818-080000.gz",
    domain_codes=["en.m"],
    page_title="Rust",
)

for row in rows:
    print(row)
```
