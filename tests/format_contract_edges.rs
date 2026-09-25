use std::fs;

use arrow::array::{Array, BooleanArray, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::DataType;
use basaltic_red::engine::formats::handler_for;
use basaltic_red::engine::map::resolve_map_path;
use basaltic_red::engine::MatrixEngine;
use tempfile::tempdir;

fn read_batches(ext: &str, path: &std::path::Path) -> (arrow_schema::SchemaRef, Vec<RecordBatch>) {
    let source = handler_for(ext)
        .unwrap()
        .open(path.to_str().unwrap(), 1024)
        .unwrap();
    let schema = source.schema.clone();
    let batches = source.batches.map(|batch| batch.unwrap()).collect();
    (schema, batches)
}

fn nested_active_values(batch: &RecordBatch) -> Vec<Option<bool>> {
    let nested = batch
        .column_by_name("nested")
        .unwrap()
        .as_any()
        .downcast_ref::<StructArray>()
        .unwrap();
    let active = nested
        .column_by_name("active")
        .unwrap()
        .as_any()
        .downcast_ref::<BooleanArray>()
        .unwrap();
    (0..active.len())
        .map(|index| (!active.is_null(index)).then(|| active.value(index)))
        .collect()
}

#[test]
fn delimited_handlers_share_header_quote_crlf_and_eof_contracts() {
    let dir = tempdir().unwrap();

    for (ext, delimiter) in [("csv", ','), ("tsv", '\t'), ("psv", '|'), ("txt", ';')] {
        let path = dir.path().join(format!("contract.{ext}"));
        let contents = format!(
            "id{delimiter}note\r\n1{delimiter}\"line one\r\nline \"\"quoted\"\"\"\r\n2{delimiter}tail"
        );
        fs::write(&path, contents).unwrap();

        let (schema, batches) = read_batches(ext, &path);
        assert_eq!(schema.field(0).name(), "id", ".{ext} header");
        assert_eq!(schema.field(1).name(), "note", ".{ext} header");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);

        let notes = batches[0]
            .column_by_name("note")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(notes.value(0), "line one\r\nline \"quoted\"");
        assert_eq!(notes.value(1), "tail");
    }
}

#[test]
fn delimited_handlers_treat_empty_and_header_only_inputs_as_zero_rows() {
    let dir = tempdir().unwrap();

    for (ext, delimiter) in [("csv", ','), ("tsv", '\t'), ("psv", '|'), ("txt", ';')] {
        let path = dir.path().join(format!("empty.{ext}"));
        fs::write(&path, b"").unwrap();
        let (_, batches) = read_batches(ext, &path);
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);

        fs::write(&path, format!("id{delimiter}value\r\n")).unwrap();
        let (schema, batches) = read_batches(ext, &path);
        assert_eq!(schema.fields().len(), 2, ".{ext} header schema");
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }
}

#[test]
fn delimited_handlers_reject_invalid_utf8() {
    let dir = tempdir().unwrap();
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    for (ext, delimiter) in [("csv", b','), ("tsv", b'\t'), ("psv", b'|'), ("txt", b';')] {
        let path = dir.path().join(format!("invalid.{ext}"));
        let mut contents =
            format!("id{}value\n1{}", delimiter as char, delimiter as char).into_bytes();
        contents.push(0xff);
        fs::write(&path, contents).unwrap();

        let result = handler_for(ext)
            .unwrap()
            .process_file(&engine, path.to_str().unwrap(), 1024);
        assert!(result.is_err(), ".{ext} accepted invalid UTF-8");
    }
}

#[test]
fn json_nested_arrays_preserve_schema_and_map_fallback_values() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("nested.json");
    let array = r#"[
        {"id":1,"label":"brace } and escaped \"quote\"","nested":{"active":true,"tags":["one"]}},
        {"id":2,"label":"tail","nested":{"active":false,"tags":[]}},
        {"id":3,"label":"last","nested":{"tags":["three"]}}
    ]"#;
    fs::write(&path, array).unwrap();

    for ext in ["json", "jsonl"] {
        let (schema, batches) = read_batches(ext, &path);
        let nested_field = schema.field_with_name("nested").unwrap();
        assert!(matches!(nested_field.data_type(), DataType::Struct(_)));
        assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 3);
    }

    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);
    engine
        .create_lake_map_native(dir.path().to_str().unwrap(), false)
        .unwrap();
    let mapped = engine
        .slice_rows_native(path.to_str().unwrap(), 1, 2)
        .unwrap();

    fs::remove_file(resolve_map_path(dir.path())).unwrap();
    let fallback = engine
        .slice_rows_native(path.to_str().unwrap(), 1, 2)
        .unwrap();

    let ids = |batch: &RecordBatch| {
        batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    };
    assert_eq!(ids(&mapped), vec![2, 3]);
    assert_eq!(ids(&fallback), ids(&mapped));

    let mapped_active = nested_active_values(&mapped);
    let fallback_active = nested_active_values(&fallback);
    assert_eq!(mapped_active, vec![Some(false), None]);
    assert_eq!(fallback_active, mapped_active);
}

#[test]
fn json_schema_samples_100_records_and_ignores_later_unknown_fields() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("varying.ndjson");
    let mut contents = String::new();
    for id in 0..100 {
        if id == 0 {
            contents.push_str(&format!("{{\"id\":{id},\"known\":\"first\"}}\n"));
        } else {
            contents.push_str(&format!("{{\"id\":{id}}}\n"));
        }
    }
    contents.push_str("{\"id\":100,\"late\":\"ignored\"}");
    fs::write(&path, contents).unwrap();

    let (schema, batches) = read_batches("ndjson", &path);
    assert!(schema.index_of("late").is_err());
    assert_eq!(
        batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        101
    );

    let known = batches[0]
        .column_by_name("known")
        .unwrap()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(known.value(0), "first");
    assert!(known.is_null(1));
}

#[test]
fn json_rejects_a_type_conflict_after_the_inference_sample() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("late_type_conflict.ndjson");
    let mut contents = String::new();
    for id in 0..100 {
        contents.push_str(&format!("{{\"id\":{id}}}\n"));
    }
    contents.push_str("{\"id\":\"not a number\"}");
    fs::write(&path, contents).unwrap();

    let source = handler_for("ndjson")
        .unwrap()
        .open(path.to_str().unwrap(), 1024)
        .unwrap();
    let result: Result<Vec<_>, _> = source.batches.collect();
    assert!(result.is_err());
}

#[test]
fn json_line_handlers_reject_a_malformed_final_record() {
    let dir = tempdir().unwrap();

    for ext in ["json", "jsonl", "ndjson"] {
        let path = dir.path().join(format!("malformed.{ext}"));
        fs::write(&path, "{\"id\":1}\n{\"id\":").unwrap();

        let result = match handler_for(ext).unwrap().open(path.to_str().unwrap(), 1024) {
            Err(error) => Err(error),
            Ok(source) => source.batches.collect::<Result<Vec<_>, _>>().map(|_| ()),
        };
        assert!(result.is_err(), ".{ext} accepted a malformed final record");
    }
}

#[test]
fn json_array_handlers_reject_invalid_separators_and_trailing_content() {
    let dir = tempdir().unwrap();
    let engine = MatrixEngine::new(1, 9, 0.01, 100.0);

    for ext in ["json", "jsonl"] {
        for (index, contents) in [
            "[{\"id\":1}{\"id\":2}]",
            "[{\"id\":1},]",
            "[{\"id\":1}] trailing",
        ]
        .into_iter()
        .enumerate()
        {
            let path = dir.path().join(format!("invalid_{index}.{ext}"));
            fs::write(&path, contents).unwrap();
            let result =
                handler_for(ext)
                    .unwrap()
                    .process_file(&engine, path.to_str().unwrap(), 1024);
            assert!(result.is_err(), ".{ext} accepted {contents}");
        }
    }
}
