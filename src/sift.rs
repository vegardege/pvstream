use regex::Regex;

/// Filters raw lines by a regular expression.
///
/// Optional filter for lines from the pageviews file. Applied before parsing,
/// which makes it possible to significantly reduce the amount of parsing in
/// cases where we're only looking for a subset of the file.
pub fn is_valid_line<E>(
    line_regex: Option<Regex>,
) -> Box<dyn Fn(&Result<String, E>) -> bool + Send + Sync> {
    match line_regex {
        Some(regex) => Box::new(move |line| match line {
            Ok(line) => regex.is_match(&line),
            Err(_) => true, // Pass through to handle later
        }),
        None => Box::new(|_| true),
    }
}
