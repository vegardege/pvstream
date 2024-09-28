from collections.abc import Generator
from typing import Optional

class Pageviews:  # noqa: E302
    domain_code: str
    page_title: str
    views: int
    language: str
    domain: Optional[str]
    mobile: bool

def stream_from_file(  # noqa: E302
    path,
    line_regex: Optional[str] = None,
    domain_codes: Optional[list[str]] = None,
    page_title: Optional[str] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    languages: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    mobile: Optional[bool] = None,
) -> Generator[Pageviews, None, None]: ...
def stream_from_url(  # noqa: E302
    url,
    line_regex: Optional[str] = None,
    domain_codes: Optional[list[str]] = None,
    page_title: Optional[str] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    languages: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    mobile: Optional[bool] = None,
) -> Generator[Pageviews, None, None]: ...
def parquet_from_file(  # noqa: E302
    input_path: str,
    output_path: str,
    batch_size: Optional[int] = None,
    line_regex: Optional[str] = None,
    domain_codes: Optional[list[str]] = None,
    page_title: Optional[str] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    languages: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    mobile: Optional[bool] = None,
) -> None: ...
def parquet_from_url(  # noqa: E302
    url: str,
    output_path: str,
    batch_size: Optional[int] = None,
    line_regex: Optional[str] = None,
    domain_codes: Optional[list[str]] = None,
    page_title: Optional[str] = None,
    min_views: Optional[int] = None,
    max_views: Optional[int] = None,
    languages: Optional[list[str]] = None,
    domains: Optional[list[str]] = None,
    mobile: Optional[bool] = None,
) -> None: ...
