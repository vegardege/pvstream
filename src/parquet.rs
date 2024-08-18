use std::sync::Arc;

use crate::parse::{Pageviews, ParseError};
use arrow2::array::{Array, MutableBooleanArray, MutablePrimitiveArray, MutableUtf8Array};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};

/// Creates the arrow schema used for flattened structs.
///
/// As in the python bindings, we flatten this to make it easier to work with.
fn create_schema() -> Schema {
    Schema::from(vec![
        Field::new("domain_code", DataType::Utf8, false),
        Field::new("page_title", DataType::Utf8, false),
        Field::new("views", DataType::UInt32, false),
        Field::new("language", DataType::Utf8, false),
        Field::new("domain", DataType::Utf8, true),
        Field::new("mobile", DataType::Boolean, false),
    ])
}

/// Convert the iterator of structs to an arrow chunk.
///
/// Note that the entire dataset will be moved to memory if you call this
/// function, but it's basically the best you can do if you want to work
/// on it in memory.
pub fn arrow_from_structs(
    iterator: impl Iterator<Item = Result<Pageviews, ParseError>>,
) -> Chunk<Arc<dyn Array>> {
    let mut domain_code_builder = MutableUtf8Array::<i32>::new();
    let mut page_title_builder = MutableUtf8Array::<i32>::new();
    let mut views_builder = MutablePrimitiveArray::<u32>::new();
    let mut language_builder = MutableUtf8Array::<i32>::new();
    let mut domain_builder = MutableUtf8Array::<i32>::new();
    let mut mobile_builder = MutableBooleanArray::new();

    for element in iterator {
        if let Ok(row) = element {
            domain_code_builder.push(Some(&row.domain_code));
            page_title_builder.push(Some(&row.page_title));
            views_builder.push(Some(row.views));
            language_builder.push(Some(&row.parsed_domain_code.language));
            domain_builder.push(row.parsed_domain_code.domain);
            mobile_builder.push(Some(row.parsed_domain_code.mobile));
        }
    }

    Chunk::new(vec![
        domain_code_builder.into_arc(),
        page_title_builder.into_arc(),
        views_builder.into_arc(),
        language_builder.into_arc(),
        domain_builder.into_arc(),
        mobile_builder.into_arc(),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::DomainCode;
    use crate::parse::ParseError;
    use arrow2::array::{BooleanArray, UInt32Array, Utf8Array};

    fn make_pageviews() -> Vec<Result<Pageviews, ParseError>> {
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

        vec![Ok(pv1), Ok(pv2)]
    }

    #[test]
    fn test_arrow_from_structs() {
        let pageviews = make_pageviews().into_iter();
        let chunk = arrow_from_structs(pageviews);

        // Test array size (2 rows, 6 columns)
        assert_eq!(chunk.arrays().len(), 6);
        assert_eq!(chunk.len(), 2);

        // Test values of first row
        let domain_code_array = chunk.arrays()[0]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert_eq!(domain_code_array.value(0), "en");
        assert_eq!(domain_code_array.value(1), "de.m");

        let page_title_array = chunk.arrays()[1]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert_eq!(page_title_array.value(0), "Main_Page");
        assert_eq!(page_title_array.value(1), "Startseite");

        let views_array = chunk.arrays()[2]
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(views_array.value(0), 1000);
        assert_eq!(views_array.value(1), 500);

        let language_array = chunk.arrays()[3]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert_eq!(language_array.value(0), "en");
        assert_eq!(language_array.value(1), "de");

        let domain_array = chunk.arrays()[4]
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        assert_eq!(domain_array.value(0), "wikipedia.org");
        assert_eq!(domain_array.value(1), "wikipedia.de");

        let domain_array = chunk.arrays()[5]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(domain_array.value(0), false);
        assert_eq!(domain_array.value(1), true);
    }
}
