use regex::Regex;

use crate::parse::Pageviews;

#[derive(Clone, Default)]
pub struct Filters {
    pub line_regex: Option<Regex>,
    pub mobile: Option<bool>,
}

impl Filters {
    fn has_obj_filters(self: &Self) -> bool {
        self.mobile.is_some()
    }

    pub fn matches_obj(&self, obj: &Pageviews) -> bool {
        if let Some(expected_mobile) = self.mobile {
            if obj.parsed_domain_code.mobile != expected_mobile {
                return false;
            }
        }
        true
    }
}

/// Filters raw lines by a regular expression.
///
/// Optional filter for lines from the pageviews file. Applied before parsing,
/// which makes it possible to significantly reduce the amount of parsing in
/// cases where we're only looking for a subset of the file.
pub fn pre_filter<E>(filters: &Filters) -> Box<dyn Fn(&Result<String, E>) -> bool + Send + Sync> {
    let line_regex = filters.line_regex.clone();
    match line_regex {
        Some(regex) => {
            Box::new(move |line| match line {
                Ok(line) => regex.is_match(&line),
                Err(_) => true, // Pass through to handle later
            })
        }
        None => Box::new(|_| true),
    }
}

pub fn post_filter<E>(
    filters: &Filters,
) -> Box<dyn Fn(&Result<Pageviews, E>) -> bool + Send + Sync> {
    if filters.has_obj_filters() {
        let filters = filters.clone();
        Box::new(move |result| match result {
            Ok(obj) => filters.matches_obj(obj),
            Err(_) => true, // Pass through to handle later
        })
    } else {
        Box::new(|_| true)
    }
}
