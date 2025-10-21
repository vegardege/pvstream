# pvstream

[![Code Quality](https://github.com/vegardege/pvstream/actions/workflows/code-quality.yml/badge.svg)](https://github.com/vegardege/pvstream/actions/workflows/code-quality.yml)

`pvstream` is a Rust library with python bindings allowing you to efficiently
stream download, parse, and filter pageview from Wikimedia's hourly dumps.

The library can be used from Rust or python. In both languages you can choose
between an iterator of parsed objects, made available on the fly as the file
is downloaded, or a complete parquet file of parsed and filtered data.

## Installation

To use `pvstream` in your Rust project, add it to your `Cargo.toml`:

```toml
[dependencies]
pvstream = { git = "https://github.com/vegardege/pvstream" }
```

To use `pvstream` in a python project, run:

```python
pip install pvstream
```

To build for your hardware, run this in your virtual environment:

```python
pip install maturin
git clone https://github.com/vegardege/pvstream
cd pvstream
maturin develop --release
```

Or run:

```python
maturin build --release
```

and `pip install` from `target/wheels`.

I have not put a lot of effort into making installation easy on different
architectures. Get in touch if you want to help out.

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
> more than once. Consider using a mirror closer to you by. You can find
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
