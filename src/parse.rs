use std::collections::HashMap;
use std::sync::LazyLock;
use thiserror::Error;

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

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Field '{0}' was not found:\n{1}")]
    MissingField(&'static str, String),

    #[error("Invalid '{0}':\n{1}")]
    InvalidField(&'static str, String),

    #[error(transparent)]
    ReadError(#[from] std::io::Error),
}

fn missing(field: &'static str, line: &str) -> ParseError {
    ParseError::MissingField(field, line.to_string())
}

fn invalid(field: &'static str, line: &str) -> ParseError {
    ParseError::InvalidField(field, line.to_string())
}

#[derive(Debug)]
pub struct DomainCode {
    pub language: String,
    pub domain: Option<&'static str>,
    pub mobile: bool,
}

#[derive(Debug)]
pub struct Pageviews {
    pub domain_code: String,
    pub page_title: String,
    pub views: u32,
    pub parsed_domain_code: DomainCode,
}

/// Normalizes a string in the Wikimedia custom file format.
///
/// The files contain four space separated columns. For some reason, strings may
/// be contained in a "". This only appears to happen for some empty strings and
/// for strings containing a ", which is escaped to \". This behavior is not
/// explicitly documented, so this function may have to be revised.
fn normalize_string(value: &str) -> String {
    if value.starts_with('"') && value.ends_with('"') {
        value[1..value.len() - 1].replace(r#"\""#, r#"""#)
    } else {
        value.to_string()
    }
}

/// Parses a Wikimedia domain code into language, project domain, and mobile flag.
///
/// Domain codes follow the pattern defined by the Wikimedia traffic pipeline:
/// https://wikitech.wikimedia.org/wiki/Data_Platform/Data_Lake/Traffic/Pageviews
fn parse_domain_code(domain_code: &str) -> Result<DomainCode, ParseError> {
    // The domain code is split in 1-3 parts, separated by periods. These parts
    // will not always have the same meaning, hence the non-descriptive names.
    let mut parts = domain_code.splitn(3, '.');

    let first = parts
        .next()
        .ok_or_else(|| invalid("domain code", domain_code))?;
    let second = parts.next();
    let third = parts.next();

    match (first, second, third) {
        // As an edge case, domain codes starting with a white listed Wikimedia
        // project name follows a separate pattern, e.g. "commons.m" for the
        // non-mobile site or "commons.m.m" for the mobile site.
        (project, _, _) if WIKIMEDIA_PROJECTS.contains_key(project) => Ok(DomainCode {
            language: "en".to_string(),
            domain: WIKIMEDIA_PROJECTS.get(project).copied(),
            mobile: third.is_some(),
        }),
        // A weird edge case where the domain_code is only a quoted blank
        // string. It appears to be wikifunctions, but is not documented.
        (r#""""#, None, None) => Ok(DomainCode {
            language: "en".to_string(),
            domain: Some("wikifunctions.org"),
            mobile: false,
        }),
        // If we only get one part, it's always a language code from a
        // non-mobile wikipedia.org page, e.g. "en" or "no".
        (language, None, None) => Ok(DomainCode {
            language: language.into(),
            domain: Some("wikipedia.org"),
            mobile: false,
        }),
        // Two parts, one of which is "m" or "zero", is a mobile page on
        // wikipedia.org, e.g. "en.m" or "no.zero".
        (language, Some("m" | "zero"), None) => Ok(DomainCode {
            language: language.into(),
            domain: Some("wikipedia.org"),
            mobile: true,
        }),
        // Two parts without one of the mobile markers is a non-mobile page
        // from a Wikimedia project other than wikipedia.org, e.g. "en.b"
        // for "en.wikibooks.org".
        (language, Some(code), None) => Ok(DomainCode {
            language: language.into(),
            domain: DOMAINS.get(code).copied(),
            mobile: false,
        }),
        // Three parts is a mobile page from a Wikimedia project other than
        // wikipedia.org, e.g. "en.m.b" for "en.m.wikibooks.org".
        (language, Some(_), Some(code)) => Ok(DomainCode {
            language: language.into(),
            domain: DOMAINS.get(code).copied(),
            mobile: true,
        }),
        // Unreachable fallback.
        _ => Err(invalid("domain code", domain_code)),
    }
}

/// Parses a single line from a Wikimedia pageviews file.
///
/// The file is space separated with four columns, two strings and two
/// numbers. The strings can be quoted with escapes for the quote sign.
/// The first column, domain code, is a dot separated string, which is
/// broken into subcomponents in the returned struct.
pub fn parse_line(line: String) -> Result<Pageviews, ParseError> {
    let mut parts = line.splitn(4, ' ');

    let domain_code = parts
        .next()
        .ok_or_else(|| missing("domain code", &line))?
        .to_owned();
    let page_title_raw = parts.next().ok_or_else(|| missing("page title", &line))?;
    let views = parts
        .next()
        .ok_or_else(|| missing("views", &line))?
        .parse()
        .map_err(|_| invalid("views", &line))?;

    let page_title = normalize_string(page_title_raw);
    let parsed_domain_code = parse_domain_code(&domain_code)?;

    Ok(Pageviews {
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
        assert_eq!(result.domain, Some("wikipedia.org".into()));
        assert!(!result.mobile);
    }

    #[test]
    fn test_wikipedia_mobile() {
        let result = parse_domain_code("no.m").unwrap();
        assert_eq!(result.language, "no");
        assert_eq!(result.domain, Some("wikipedia.org".into()));
        assert!(result.mobile);
    }

    #[test]
    fn test_other_project() {
        let result = parse_domain_code("fr.v").unwrap();
        assert_eq!(result.language, "fr");
        assert_eq!(result.domain, Some("wikiversity.org".into()));
        assert!(!result.mobile);
    }

    #[test]
    fn test_other_project_mobile() {
        let result = parse_domain_code("fr.m.v").unwrap();
        assert_eq!(result.language, "fr");
        assert_eq!(result.domain, Some("wikiversity.org".into()));
        assert!(result.mobile);
    }

    #[test]
    fn test_wikimedia_project() {
        let result = parse_domain_code("commons.m").unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("commons.wikimedia.org".into()));
        assert!(!result.mobile);
    }

    #[test]
    fn test_wikimedia_mobile() {
        let result = parse_domain_code("meta.m.m").unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("meta.wikimedia.org".into()));
        assert!(result.mobile);
    }

    #[test]
    fn test_empty_quotes_domain_code() {
        let result = parse_domain_code(r#""""#).unwrap();
        assert_eq!(result.language, "en");
        assert_eq!(result.domain, Some("wikifunctions.org".into()));
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
        let result = parse_line("en.m Copenhagen 54 0".into()).unwrap();
        assert_eq!(result.domain_code, "en.m");
        assert_eq!(result.page_title, "Copenhagen");
        assert_eq!(result.views, 54);
        assert_eq!(result.parsed_domain_code.language, "en");
        assert_eq!(
            result.parsed_domain_code.domain,
            Some("wikipedia.org".into())
        );
        assert!(result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_utf8_line() {
        let result = parse_line(r"ja \(^o^)/チエ 1 0".into()).unwrap();
        assert_eq!(result.domain_code, "ja");
        assert_eq!(result.page_title, r"\(^o^)/チエ");
        assert_eq!(result.views, 1);
        assert_eq!(result.parsed_domain_code.language, "ja");
        assert_eq!(
            result.parsed_domain_code.domain,
            Some("wikipedia.org".into())
        );
        assert!(!result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_quoted_line() {
        let result =
            parse_line(r#"vi.m "\"Hello,_World!\"_(chương_trình_máy_tính)" 1 0"#.into()).unwrap();
        assert_eq!(result.domain_code, "vi.m");
        assert_eq!(
            result.page_title,
            r#""Hello,_World!"_(chương_trình_máy_tính)"#
        );
        assert_eq!(result.views, 1);
        assert_eq!(result.parsed_domain_code.language, "vi");
        assert_eq!(
            result.parsed_domain_code.domain,
            Some("wikipedia.org".into())
        );
        assert!(result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_wikibooks_line() {
        let result = parse_line("uk.b Ядро_Linux/Модулі 2 0".into()).unwrap();
        assert_eq!(result.domain_code, "uk.b");
        assert_eq!(result.page_title, "Ядро_Linux/Модулі");
        assert_eq!(result.views, 2);
        assert_eq!(result.parsed_domain_code.language, "uk");
        assert_eq!(
            result.parsed_domain_code.domain,
            Some("wikibooks.org".into())
        );
        assert!(!result.parsed_domain_code.mobile);
    }

    #[test]
    fn test_missing_fields() {
        let missing_page_title = parse_line("".into()).unwrap_err();
        assert!(matches!(
            missing_page_title,
            ParseError::MissingField("page title", _)
        ));

        let missing_views = parse_line("en.m Hello_World".into()).unwrap_err();
        assert!(matches!(
            missing_views,
            ParseError::MissingField("views", _)
        ));
    }

    #[test]
    fn test_invalid_fields() {
        // Invalid domain code is currently unreachable. Maybe we should be
        // stricter about validating it and returning errors, but I suspect
        // it's better to be flexible about the format.

        let invalid_views = parse_line("en.m Hello World 1 0".into()).unwrap_err();
        assert!(matches!(
            invalid_views,
            ParseError::InvalidField("views", _)
        ));
    }
}
