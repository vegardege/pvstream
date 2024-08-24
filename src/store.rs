use crate::parse::{Pageviews, ParseError};
use arrow2::array::TryPush;
use arrow2::array::{
    Array, MutableBooleanArray, MutableDictionaryArray, MutablePrimitiveArray, MutableUtf8Array,
};
use arrow2::chunk::Chunk;
use arrow2::datatypes::{DataType, Field, Schema};
use arrow2::io::parquet::write::*;
use std::fs::File;
use std::path::Path;
use std::sync::Arc;

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

/// Batches parsed rows to output efficiently to the parquet file.
///
/// Writing one row at a time is unuseably inefficient when working with
/// parquet files. Writing the entire batch in one go is the fastest,
/// but requires us to keep the whole file in memory at once, in addition
/// to internal objects. The iterator can be used to find the sweet spot
/// for a user's specific use case.
struct ChunkIterator<I: Iterator<Item = Result<Pageviews, ParseError>>> {
    iter: I,
    batch_size: usize,
}

impl<I: Iterator<Item = Result<Pageviews, ParseError>>> Iterator for ChunkIterator<I> {
    type Item = Result<Chunk<Arc<dyn Array>>, arrow2::error::Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut domain_code_builder: MutableDictionaryArray<i32, MutableUtf8Array<i32>> =
            MutableDictionaryArray::new();
        let mut page_title_builder = MutableUtf8Array::<i32>::new();
        let mut views_builder = MutablePrimitiveArray::<u32>::new();
        let mut language_builder: MutableDictionaryArray<i32, MutableUtf8Array<i32>> =
            MutableDictionaryArray::new();
        let mut domain_builder: MutableDictionaryArray<i32, MutableUtf8Array<i32>> =
            MutableDictionaryArray::new();
        let mut mobile_builder = MutableBooleanArray::new();

        let mut count = 0;

        while count < self.batch_size {
            match self.iter.next() {
                Some(Ok(row)) => {
                    if domain_code_builder
                        .try_push(Some(&row.domain_code))
                        .is_err()
                        || language_builder
                            .try_push(Some(&row.parsed_domain_code.language))
                            .is_err()
                        || domain_builder
                            .try_push(row.parsed_domain_code.domain)
                            .is_err()
                    {
                        // If `try_push` fails, the mutable builders are
                        // potentially in a corrupted state, and we need
                        // to abandon the entire Chunk.
                        return None;
                    }

                    page_title_builder.push(Some(&row.page_title));
                    views_builder.push(Some(row.views));
                    mobile_builder.push(Some(row.parsed_domain_code.mobile));

                    count += 1;
                }
                Some(Err(_)) => {
                    // Skip rows with parse errors
                    continue;
                }
                None => break,
            }
        }

        if count == 0 {
            None
        } else {
            Some(Ok(Chunk::new(vec![
                domain_code_builder.into_arc(),
                page_title_builder.into_arc(),
                views_builder.into_arc(),
                language_builder.into_arc(),
                domain_builder.into_arc(),
                mobile_builder.into_arc(),
            ])))
        }
    }
}

/// Converts the iterator of structs to an arrow chunk.
///
/// By default, the function splits the row into chunks equaling the default
/// parquet row group size. This gives us a bigger memory overhead than if
/// we split it into smaller groups, but the performance gain makes up for
/// it. If you're in an extremely memory constrained environment, reduce the
/// batch size.
pub fn arrow_chunks_from_structs(
    iterator: impl Iterator<Item = Result<Pageviews, ParseError>>,
    batch_size: Option<usize>,
) -> impl Iterator<Item = Result<Chunk<Arc<dyn Array>>, arrow2::error::Error>> {
    // Default to parquet row group default size
    let batch_size = batch_size.unwrap_or(122_880);

    ChunkIterator {
        iter: iterator,
        batch_size,
    }
}

/// Writes an arrow chunk to a parquet file using an iterator.
///
/// For each chunk provided by the input, the function will update a parquet
/// file. The file will be overwritten if it already exists.
///
/// RLE dictionaries are used for the string fields with few, repeated values,
/// while plain fields are used for the rest.
pub fn parquet_from_arrow<I>(path: &Path, chunks: I) -> arrow2::error::Result<()>
where
    I: Iterator<Item = Result<Chunk<Arc<dyn Array>>, arrow2::error::Error>>,
{
    let file = File::create(path)?;
    let schema = create_schema();
    let options = WriteOptions {
        write_statistics: false,
        compression: CompressionOptions::Uncompressed,
        version: Version::V2,
        data_pagesize_limit: None,
    };
    let encodings = vec![
        vec![Encoding::RleDictionary], // domain_code
        vec![Encoding::Plain],         // page_title
        vec![Encoding::Plain],         // views
        vec![Encoding::RleDictionary], // language
        vec![Encoding::RleDictionary], // domain
        vec![Encoding::Plain],         // mobile
    ];

    let row_groups = RowGroupIterator::try_new(chunks, &schema, options, encodings)?;

    let mut writer = FileWriter::try_new(file, schema, options)?;

    for group in row_groups {
        writer.write(group?)?;
    }
    writer.end(None)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parse::DomainCode;
    use crate::parse::ParseError;
    use arrow2::array::{BooleanArray, DictionaryArray, UInt32Array, Utf8Array};

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

    fn dict_lookup(dict: &DictionaryArray<i32>, idx: usize) -> &str {
        let key: usize = dict.keys().value(idx) as usize;
        let values = dict
            .values()
            .as_any()
            .downcast_ref::<Utf8Array<i32>>()
            .unwrap();
        values.value(key)
    }

    #[test]
    fn test_arrow_from_structs() {
        let pageviews = make_pageviews().into_iter();
        let chunk = arrow_chunks_from_structs(pageviews, None)
            .next()
            .unwrap()
            .unwrap();

        // Test array size (2 rows, 6 columns)
        assert_eq!(chunk.arrays().len(), 6);
        assert_eq!(chunk.len(), 2);

        // Test values of first row
        let domain_code_array = chunk.arrays()[0]
            .as_any()
            .downcast_ref::<DictionaryArray<i32>>()
            .unwrap();

        assert_eq!(dict_lookup(&domain_code_array, 0), "en");
        assert_eq!(dict_lookup(&domain_code_array, 1), "de.m");

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
            .downcast_ref::<DictionaryArray<i32>>()
            .unwrap();
        assert_eq!(dict_lookup(&language_array, 0), "en");
        assert_eq!(dict_lookup(&language_array, 1), "de");

        let domain_array = chunk.arrays()[4]
            .as_any()
            .downcast_ref::<DictionaryArray<i32>>()
            .unwrap();
        assert_eq!(dict_lookup(&domain_array, 0), "wikipedia.org");
        assert_eq!(dict_lookup(&domain_array, 1), "wikipedia.de");

        let mobile_array = chunk.arrays()[5]
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        assert_eq!(mobile_array.value(0), false);
        assert_eq!(mobile_array.value(1), true);
    }
}
