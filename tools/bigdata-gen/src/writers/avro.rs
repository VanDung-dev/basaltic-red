use std::fs::File;
use std::io::BufWriter;
use anyhow::Result;
use apache_avro::types::Value;
use apache_avro::Writer as AvroWriter;
use arrow::array::*;

use crate::gen::chunk_iter;
use crate::progress::ProgressItem;

pub fn write_avro(path: &str, seed: u64, total: u64, cols: usize, progress: &ProgressItem) -> Result<()> {
    let raw_schema = r#"{
        "type": "record",
        "name": "Record",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "uuid", "type": "string"},
            {"name": "first_name", "type": "string"},
            {"name": "last_name", "type": "string"},
            {"name": "email", "type": "string"},
            {"name": "age", "type": "int"},
            {"name": "gender", "type": "string"},
            {"name": "occupation", "type": "string"},
            {"name": "company", "type": "string"},
            {"name": "country", "type": "string"},
            {"name": "city", "type": "string"},
            {"name": "street", "type": "string"},
            {"name": "phone", "type": "string"},
            {"name": "salary", "type": "double"},
            {"name": "bonus", "type": "double"},
            {"name": "currency", "type": "string"},
            {"name": "department", "type": "string"},
            {"name": "join_date", "type": "string"},
            {"name": "is_active", "type": "boolean"},
            {"name": "score", "type": "double"},
            {"name": "rating", "type": "int"},
            {"name": "category", "type": "string"},
            {"name": "status", "type": "string"},
            {"name": "priority", "type": "string"},
            {"name": "description", "type": "string"},
            {"name": "notes", "type": "string"},
            {"name": "created_at", "type": "string"},
            {"name": "updated_at", "type": "string"},
            {"name": "ip_address", "type": "string"},
            {"name": "user_agent", "type": "string"}
        ]
    }"#;

    let avro_schema = apache_avro::Schema::parse_str(raw_schema)?;
    let file = BufWriter::new(File::create(path)?);
    let mut writer = AvroWriter::new(&avro_schema, file);

    for batch in chunk_iter(seed, total, cols) {

        let n = batch.num_rows();

        let col_id = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let col_uuid = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        let col_first_name = batch.column(2).as_any().downcast_ref::<StringArray>().unwrap();
        let col_last_name = batch.column(3).as_any().downcast_ref::<StringArray>().unwrap();
        let col_email = batch.column(4).as_any().downcast_ref::<StringArray>().unwrap();
        let col_age = batch.column(5).as_any().downcast_ref::<Int32Array>().unwrap();
        let col_gender = batch.column(6).as_any().downcast_ref::<StringArray>().unwrap();
        let col_occupation = batch.column(7).as_any().downcast_ref::<StringArray>().unwrap();
        let col_company = batch.column(8).as_any().downcast_ref::<StringArray>().unwrap();
        let col_country = batch.column(9).as_any().downcast_ref::<StringArray>().unwrap();
        let col_city = batch.column(10).as_any().downcast_ref::<StringArray>().unwrap();
        let col_street = batch.column(11).as_any().downcast_ref::<StringArray>().unwrap();
        let col_phone = batch.column(12).as_any().downcast_ref::<StringArray>().unwrap();
        let col_salary = batch.column(13).as_any().downcast_ref::<Float64Array>().unwrap();
        let col_bonus = batch.column(14).as_any().downcast_ref::<Float64Array>().unwrap();
        let col_currency = batch.column(15).as_any().downcast_ref::<StringArray>().unwrap();
        let col_department = batch.column(16).as_any().downcast_ref::<StringArray>().unwrap();
        let col_join_date = batch.column(17).as_any().downcast_ref::<StringArray>().unwrap();
        let col_is_active = batch.column(18).as_any().downcast_ref::<BooleanArray>().unwrap();
        let col_score = batch.column(19).as_any().downcast_ref::<Float64Array>().unwrap();
        let col_rating = batch.column(20).as_any().downcast_ref::<Int32Array>().unwrap();
        let col_category = batch.column(21).as_any().downcast_ref::<StringArray>().unwrap();
        let col_status = batch.column(22).as_any().downcast_ref::<StringArray>().unwrap();
        let col_priority = batch.column(23).as_any().downcast_ref::<StringArray>().unwrap();
        let col_description = batch.column(24).as_any().downcast_ref::<StringArray>().unwrap();
        let col_notes = batch.column(25).as_any().downcast_ref::<StringArray>().unwrap();
        let col_created_at = batch.column(26).as_any().downcast_ref::<StringArray>().unwrap();
        let col_updated_at = batch.column(27).as_any().downcast_ref::<StringArray>().unwrap();
        let col_ip_address = batch.column(28).as_any().downcast_ref::<StringArray>().unwrap();
        let col_user_agent = batch.column(29).as_any().downcast_ref::<StringArray>().unwrap();

        for row_idx in 0..n {
            let record = Value::Record(vec![
                ("id".to_string(),          col_id.value(row_idx).into()),
                ("uuid".to_string(),        col_uuid.value(row_idx).into()),
                ("first_name".to_string(),  col_first_name.value(row_idx).into()),
                ("last_name".to_string(),   col_last_name.value(row_idx).into()),
                ("email".to_string(),       col_email.value(row_idx).into()),
                ("age".to_string(),         col_age.value(row_idx).into()),
                ("gender".to_string(),      col_gender.value(row_idx).into()),
                ("occupation".to_string(),  col_occupation.value(row_idx).into()),
                ("company".to_string(),     col_company.value(row_idx).into()),
                ("country".to_string(),     col_country.value(row_idx).into()),
                ("city".to_string(),        col_city.value(row_idx).into()),
                ("street".to_string(),      col_street.value(row_idx).into()),
                ("phone".to_string(),       col_phone.value(row_idx).into()),
                ("salary".to_string(),      col_salary.value(row_idx).into()),
                ("bonus".to_string(),       col_bonus.value(row_idx).into()),
                ("currency".to_string(),    col_currency.value(row_idx).into()),
                ("department".to_string(),  col_department.value(row_idx).into()),
                ("join_date".to_string(),   col_join_date.value(row_idx).into()),
                ("is_active".to_string(),   col_is_active.value(row_idx).into()),
                ("score".to_string(),       col_score.value(row_idx).into()),
                ("rating".to_string(),      col_rating.value(row_idx).into()),
                ("category".to_string(),    col_category.value(row_idx).into()),
                ("status".to_string(),      col_status.value(row_idx).into()),
                ("priority".to_string(),    col_priority.value(row_idx).into()),
                ("description".to_string(), col_description.value(row_idx).into()),
                ("notes".to_string(),       col_notes.value(row_idx).into()),
                ("created_at".to_string(),  col_created_at.value(row_idx).into()),
                ("updated_at".to_string(),  col_updated_at.value(row_idx).into()),
                ("ip_address".to_string(),  col_ip_address.value(row_idx).into()),
                ("user_agent".to_string(),  col_user_agent.value(row_idx).into()),
            ]);
            writer.append(record)?;
        }
        progress.add_rows(n as u64);
        if let Ok(md) = std::fs::metadata(path) {
            progress.set_bytes(md.len());
        }
    }
    writer.flush()?;
    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}
