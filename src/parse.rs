use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::LazyLock;

static DOMAINS: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    HashMap::from([
        ("b", "wikibooks.org"),
        ("d", "wiktionary.org"),
        ("f", "wikimediafoundation.org"),
        ("m", "wikimedia.org"),
        ("n", "wikinews.org"),
        ("q", "wikiquote.org"),
        ("s", "wikisource.org"),
        ("v", "wikiversity.org"),
        ("voy", "wikivoyage.org"),
        ("w", "mediawiki.org"),
        ("wd", "wikidata.org"),
    ])
});

static WIKIMEDIA_PROJECTS: LazyLock<HashMap<&'static str, &'static str>> = LazyLock::new(|| {
    HashMap::from([
        ("commons", "commons.wikimedia.org"),
        ("meta", "meta.wikimedia.org"),
        ("incubator", "incubator.wikimedia.org"),
        ("species", "species.wikimedia.org"),
        ("strategy", "strategy.wikimedia.org"),
        ("outreach", "outreach.wikimedia.org"),
        ("usability", "usability.wikimedia.org"),
        ("quality", "quality.wikimedia.org"),
    ])
});

#[derive(Debug)]
pub struct DomainCode<'a> {
    pub language: &'a str,
    pub domain: Option<&'a str>,
    pub mobile: bool,
}

#[derive(Debug)]
pub struct PageviewsRow<'a> {
    pub domain_code: &'a str,
    pub page_title: Cow<'a, str>,
    pub views: u32,
    pub parsed_domain_code: DomainCode<'a>,
}

/// Normalizes a string in the Wikimedia custom file format.
///
/// The files contain four space separated columns. For some reason, strings may
/// be contained in a "". This only appears to happen for some empty strings and
/// for strings containing a ", which is escaped to \". This behavior is not
/// explicitly documented, so this function may have to be revised.
fn normalize_string<'a>(value: &'a str) -> Cow<'a, str> {
    if value.starts_with('"') && value.ends_with('"') {
        Cow::Owned(value[1..value.len() - 1].replace(r#"\""#, r#"""#))
    } else {
        Cow::Borrowed(value)
    }
}

/// Parses a Wikimedia domain code into language, project domain, and mobile flag.
///
/// Domain codes follow the pattern defined by the Wikimedia traffic pipeline:
/// https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews
fn parse_domain_code(domain_code: &str) -> Option<DomainCode> {
    // The domain code is split in 1-3 parts, separated by periods
    let mut parts = domain_code.splitn(3, '.');
    let first = parts.next().unwrap_or("");
    let second = parts.next();
    let third = parts.next();

    // As an edge case, domain codes starting with a white listed Wikimedia
    // project name follows a separate pattern, e.g. "commons.m" for the
    // non-mobile site or "commons.m.m" for the mobile site.
    if let Some(domain) = WIKIMEDIA_PROJECTS.get(first) {
        return Some(DomainCode {
            language: "en",
            domain: Some(domain),
            mobile: third.is_some(),
        });
    }

    match (first, second, third) {
        // A weird edge case where the domain_code is only a quoted
        // blank string. It appears to be wikifunctions, but is not
        // documented.
        (r#""""#, None, None) => Some(DomainCode {
            language: "en",
            domain: Some("wikifunctions.org"),
            mobile: false,
        }),
        // If we only get one part, it's always a language code from a
        // non-mobile wikipedia.org page, e.g. "en" or "no".
        (language, None, None) => Some(DomainCode {
            language,
            domain: Some("wikipedia.org"),
            mobile: false,
        }),
        // Two parts, one of which is "m" or "zero", is a mobile page
        // on wikipedia.org, e.g. "en.m" or "no.zero".
        (language, Some("m" | "zero"), None) => Some(DomainCode {
            language,
            domain: Some("wikipedia.org"),
            mobile: true,
        }),
        // Two parts without one of the mobile markers is a non-mobile
        // page from a Wikimedia project other than wikipedia.org, e.g.
        // "en.b" for "en.wikibooks.org".
        (language, Some(code), None) => Some(DomainCode {
            language,
            domain: DOMAINS.get(code).cloned(),
            mobile: false,
        }),
        // Three parts is a mobile page from a Wikimedia project other
        // than wikipedia.org, e.g. "en.m.b" for "en.m.wikibooks.org".
        (language, Some(_), Some(code)) => Some(DomainCode {
            language,
            domain: DOMAINS.get(code).cloned(),
            mobile: true,
        }),
        // Unreachable fallback.
        _ => None,
    }
}

/// Parses a single line from a Wikimedia pageviews file.
///
/// The file is space separated with four columns, two strings and two
/// numbers. The strings can be quoted with escapes for the quote sign.
/// The first column, domain code, is a dot separated string, which is
/// broken into subcomponents in the returned struct.
pub fn parse_line<'a>(line: &'a str) -> Result<PageviewsRow<'a>, String> {
    let mut parts = line.splitn(4, ' ');

    // We expect each line to have at least three columns.
    let domain_code = parts.next().ok_or("Missing domain code")?;
    let page_title = normalize_string(parts.next().ok_or("Missing page title")?);
    let views = parts
        .next()
        .ok_or("Missing view count")?
        .parse::<u32>()
        .map_err(|_| "Invalid view count")?;

    let parsed_domain_code = parse_domain_code(&domain_code).ok_or("Invalid domain code")?;

    Ok(PageviewsRow {
        domain_code,
        page_title,
        views,
        parsed_domain_code,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_empty_string() {
        let result = normalize_string("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_quoted_empty_string() {
        let result = normalize_string(r#""""#);
        assert_eq!(result, "");
    }

    #[test]
    fn test_normalize_normal_string() {
        let result = normalize_string("Greater_Tokyo_Area");
        assert_eq!(result, "Greater_Tokyo_Area");
    }

    #[test]
    fn test_normalize_quoted_string_with_escape() {
        let result = normalize_string(r#""Pryp\"jat'""#);
        assert_eq!(result, r#"Pryp"jat'"#);
    }

    #[test]
    fn test_wikipedia_plain() {
        let result = parse_domain_code("en").unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("wikipedia.org"));
        assert!(!result.mobile);
    }

    #[test]
    fn test_wikipedia_mobile() {
        let result = parse_domain_code("no.m").unwrap();
        assert_eq!(result.language, "no");
        assert_eq!(result.domain, Some("wikipedia.org"));
        assert!(result.mobile);
    }

    #[test]
    fn test_other_project() {
        let result = parse_domain_code("fr.v").unwrap();
        assert_eq!(result.language, "fr");
        assert_eq!(result.domain, Some("wikiversity.org"));
        assert!(!result.mobile);
    }

    #[test]
    fn test_other_project_mobile() {
        let result = parse_domain_code("fr.m.v").unwrap();
        assert_eq!(result.language, "fr");
        assert_eq!(result.domain, Some("wikiversity.org"));
        assert!(result.mobile);
    }

    #[test]
    fn test_wikimedia_project() {
        let result = parse_domain_code("commons.m").unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("commons.wikimedia.org"));
        assert!(!result.mobile);
    }

    #[test]
    fn test_wikimedia_mobile() {
        let result = parse_domain_code("meta.m.m").unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("meta.wikimedia.org"));
        assert!(result.mobile);
    }

    #[test]
    fn test_empty_quotes_domain_code() {
        let result = parse_domain_code(r#""""#).unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("wikifunctions.org"));
        assert!(!result.mobile);
    }

    #[test]
    fn test_unknown_project_fallback() {
        let result = parse_domain_code("xx.unknown").unwrap();
        assert_eq!(result.language, "xx");
        assert_eq!(result.domain, None);
        assert!(!result.mobile);
    }

    #[test]
    fn test_simple_line() {
        let result = parse_line("en.m Copenhagen 54 0").unwrap();
        assert_eq!(result.domain_code, "en.m");
        assert_eq!(result.page_title, "Copenhagen");
        assert_eq!(result.views, 54);
        assert_eq!(result.parsed_domain_code.language, "en");
        assert_eq!(result.parsed_domain_code.domain, Some("wikipedia.org"));
        assert!(result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_utf8_line() {
        let result = parse_line(r"ja \(^o^)/チエ 1 0").unwrap();
        assert_eq!(result.domain_code, "ja");
        assert_eq!(result.page_title, r"\(^o^)/チエ");
        assert_eq!(result.views, 1);
        assert_eq!(result.parsed_domain_code.language, "ja");
        assert_eq!(result.parsed_domain_code.domain, Some("wikipedia.org"));
        assert!(!result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_quoted_line() {
        let result = parse_line(r#"vi.m "\"Hello,_World!\"_(chương_trình_máy_tính)" 1 0"#).unwrap();
        assert_eq!(result.domain_code, "vi.m");
        assert_eq!(
            result.page_title,
            r#""Hello,_World!"_(chương_trình_máy_tính)"#
        );
        assert_eq!(result.views, 1);
        assert_eq!(result.parsed_domain_code.language, "vi");
        assert_eq!(result.parsed_domain_code.domain, Some("wikipedia.org"));
        assert!(result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_wikibooks_line() {
        let result = parse_line("uk.b Ядро_Linux/Модулі 2 0").unwrap();
        assert_eq!(result.domain_code, "uk.b");
        assert_eq!(result.page_title, "Ядро_Linux/Модулі");
        assert_eq!(result.views, 2);
        assert_eq!(result.parsed_domain_code.language, "uk");
        assert_eq!(result.parsed_domain_code.domain, Some("wikibooks.org"));
        assert!(!result.parsed_domain_code.mobile);
    }
}
