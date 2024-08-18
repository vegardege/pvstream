use regex::Regex;

use crate::parse::Pageviews;

/// Filter for rows/objects. Apply to restrict returned data.
///
/// By default, all rows/objects are permitted. This struct can be used to
/// restrict this, by adding filters to the various fields.
///
/// `line_regex` is applied before parsing each row, the rest are applied
/// after parsing. Use `line_regex` when possible, as it's far more efficient.
///
/// Use `FilterBuilder` for a more convenient setup.
#[derive(Clone, Default)]
pub struct Filter {
    pub line_regex: Option<Regex>,
    pub domain_codes: Option<Vec<String>>,
    pub page_title: Option<Regex>,
    pub min_views: Option<u32>,
    pub max_views: Option<u32>,
    pub languages: Option<Vec<String>>,
    pub domains: Option<Vec<String>>,
    pub mobile: Option<bool>,
}

impl Filter {
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

/// Builds a row/object filter.
///
/// By default, all rows/objects are permitted. This struct can be used to
/// restrict this, by adding filters to the various fields.
///
/// `line_regex` is applied before parsing each row, the rest are applied
/// after parsing. Use `line_regex` when possible, as it's far more efficient.
#[derive(Default)]
pub struct FilterBuilder {
    filter: Filter,
}

#[allow(dead_code)]
impl FilterBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn line_regex(mut self, pattern: &str) -> Self {
        self.filter.line_regex = Some(Regex::new(pattern).expect("Invalid regex"));
        self
    }

    pub fn domain_codes<T: Into<String>>(mut self, codes: impl IntoIterator<Item = T>) -> Self {
        self.filter.domain_codes = Some(codes.into_iter().map(Into::into).collect());
        self
    }

    pub fn page_title(mut self, pattern: &str) -> Self {
        self.filter.page_title = Some(Regex::new(pattern).expect("Invalid regex"));
        self
    }

    pub fn min_views(mut self, min: u32) -> Self {
        self.filter.min_views = Some(min);
        self
    }

    pub fn max_views(mut self, max: u32) -> Self {
        self.filter.max_views = Some(max);
        self
    }

    pub fn languages<T: Into<String>>(mut self, langs: impl IntoIterator<Item = T>) -> Self {
        self.filter.languages = Some(langs.into_iter().map(Into::into).collect());
        self
    }

    pub fn domains<T: Into<String>>(mut self, doms: impl IntoIterator<Item = T>) -> Self {
        self.filter.domains = Some(doms.into_iter().map(Into::into).collect());
        self
    }

    pub fn mobile(mut self, value: bool) -> Self {
        self.filter.mobile = Some(value);
        self
    }

    pub fn build(self) -> Filter {
        self.filter
    }
}

/// Filters raw lines by a regular expression.
///
/// Optional filter for lines from the pageviews file. Applied before parsing,
/// which makes it possible to significantly reduce the amount of parsing in
/// cases where we're only looking for a subset of the file.
pub fn pre_filter<E>(filter: &Filter) -> Box<dyn Fn(&Result<String, E>) -> bool + Send + Sync> {
    if filter.has_pre_filters() {
        let regex = filter.line_regex.clone().unwrap();
        return Box::new(move |line| match line {
            Ok(line) => regex.is_match(&line),
            Err(_) => true, // Pass through to handle later
        });
    }
    Box::new(|_| true)
}

pub fn post_filter<E>(filter: &Filter) -> Box<dyn Fn(&Result<Pageviews, E>) -> bool + Send + Sync> {
    if filter.has_post_filters() {
        let filter = filter.clone();
        return Box::new(move |result| match result {
            Ok(obj) => filter.post_filter(obj),
            Err(_) => true, // Pass through to handle later
        });
    }
    Box::new(|_| true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::DomainCode;

    fn make_lines() -> (String, String) {
        let str1 = "en Main_Page 1000 0".to_string();
        let str2 = "de.m Startseite 500 0".to_string();

        (str1, str2)
    }

    fn make_pageviews() -> (Pageviews, Pageviews) {
        let pv1 = Pageviews {
            domain_code: "en".to_string(),
            page_title: "Main_Page".to_string(),
            views: 1000,
            parsed_domain_code: DomainCode {
                language: "en".to_string(),
                domain: Some("wikipedia.org"),
                mobile: false,
            },
        };

        let pv2 = Pageviews {
            domain_code: "de.m".to_string(),
            page_title: "Startseite".to_string(),
            views: 500,
            parsed_domain_code: DomainCode {
                language: "de".to_string(),
                domain: Some("wikipedia.de"),
                mobile: true,
            },
        };

        (pv1, pv2)
    }

    #[test]
    fn test_pre_filter() {
        let (en, de) = make_lines();
        let filters = FilterBuilder::new().line_regex("Start").build();

        assert!(filters.has_pre_filters());
        assert!(!filters.has_post_filters());

        let pre = pre_filter(&filters);

        assert!(!pre(&Ok(en)));
        assert!(pre(&Ok(de)));
        assert!(pre(&Err(())));
    }

    #[test]
    fn test_default_filter() {
        let (en, de) = make_pageviews();
        let filters = FilterBuilder::new().build();

        assert!(!filters.has_pre_filters());
        assert!(!filters.has_post_filters());

        let post = post_filter(&filters);

        assert!(post(&Ok(en)));
        assert!(post(&Ok(de)));
        assert!(post(&Err(())));
    }

    #[test]
    fn test_single_filter() {
        let (en, de) = make_pageviews();
        let filters = FilterBuilder::new()
            .languages(vec!["en".to_string(), "no".to_string()])
            .build();

        assert!(!filters.has_pre_filters());
        assert!(filters.has_post_filters());

        let post = post_filter::<()>(&filters);

        assert!(post(&Ok(en)));
        assert!(!post(&Ok(de)));
    }

    #[test]
    fn test_multiple_filters() {
        let (en, de) = make_pageviews();
        let filters = FilterBuilder::new()
            .domain_codes(vec!["de.m".to_string()])
            .page_title("Start")
            .min_views(400)
            .max_views(600)
            .languages(vec!["de".to_string(), "no".to_string()])
            .domains(vec!["wikipedia.de".to_string()])
            .mobile(true)
            .build();
        let post = post_filter::<()>(&filters);

        assert!(!post(&Ok(en)));
        assert!(post(&Ok(de)));
    }
}
