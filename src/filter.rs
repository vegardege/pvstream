use regex::Regex;

use crate::parse::Pageviews;

#[derive(Clone, Default)]
pub struct Filters {
    pub line_regex: Option<Regex>,
    pub domain_codes: Option<Vec<String>>,
    pub page_title: Option<Regex>,
    pub min_views: Option<u32>,
    pub max_views: Option<u32>,
    pub languages: Option<Vec<String>>,
    pub domains: Option<Vec<String>>,
    pub mobile: Option<bool>,
}

impl Filters {
    fn has_pre_filters(self: &Self) -> bool {
        self.line_regex.is_some()
    }

    fn has_post_filters(self: &Self) -> bool {
        self.domain_codes.is_some()
            || self.page_title.is_some()
            || self.min_views.is_some()
            || self.max_views.is_some()
            || self.languages.is_some()
            || self.domains.is_some()
            || self.mobile.is_some()
    }

    fn post_filter(self: &Self, obj: &Pageviews) -> bool {
        [
            self.domain_codes
                .as_ref()
                .map(|allowed| allowed.contains(&obj.domain_code)),
            self.page_title
                .as_ref()
                .map(|regex| regex.is_match(&obj.page_title)),
            self.min_views.map(|min| obj.views >= min),
            self.max_views.map(|max| obj.views <= max),
            self.languages
                .as_ref()
                .map(|langs| langs.contains(&obj.parsed_domain_code.language)),
            self.domains.as_ref().map(|domains| {
                obj.parsed_domain_code
                    .domain
                    .as_ref()
                    .map(|d| domains.contains(&d.to_string()))
                    .unwrap_or(false)
            }),
            self.mobile
                .map(|expected| obj.parsed_domain_code.mobile == expected),
        ]
        .into_iter()
        .all(|check| check.unwrap_or(true))
    }
}

/// Filters raw lines by a regular expression.
///
/// Optional filter for lines from the pageviews file. Applied before parsing,
/// which makes it possible to significantly reduce the amount of parsing in
/// cases where we're only looking for a subset of the file.
pub fn pre_filter<E>(filters: &Filters) -> Box<dyn Fn(&Result<String, E>) -> bool + Send + Sync> {
    if filters.has_pre_filters() {
        let regex = filters.line_regex.clone().unwrap();
        Box::new(move |line| match line {
            Ok(line) => regex.is_match(&line),
            Err(_) => true, // Pass through to handle later
        })
    } else {
        Box::new(|_| true)
    }
}

pub fn post_filter<E>(
    filters: &Filters,
) -> Box<dyn Fn(&Result<Pageviews, E>) -> bool + Send + Sync> {
    if filters.has_post_filters() {
        let filters = filters.clone();
        Box::new(move |result| match result {
            Ok(obj) => filters.post_filter(obj),
            Err(_) => true, // Pass through to handle later
        })
    } else {
        Box::new(|_| true)
    }
}
