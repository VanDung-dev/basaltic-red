use std::fs::File;
use std::io::BufWriter;
use anyhow::Result;
use arrow::array::*;

use crate::gen::chunk_iter;
use crate::progress::ProgressItem;

/// Standard Pretty Printed JSON Array ([ {\n  "id": 1,\n  ... \n} ]) for human reading
pub fn write_json_pretty(path: &str, seed: u64, total: u64, cols: usize, progress: &ProgressItem) -> Result<()> {
    use std::io::Write;
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

    writer.write_all(b"[\n")?;
    let mut first = true;

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

        for i in 0..n {
            if !first {
                writer.write_all(b",\n")?;
            } else {
                first = false;
            }

            let obj = serde_json::json!({
                "id": col_id.value(i),
                "uuid": col_uuid.value(i),
                "first_name": col_first_name.value(i),
                "last_name": col_last_name.value(i),
                "email": col_email.value(i),
                "age": col_age.value(i),
                "gender": col_gender.value(i),
                "occupation": col_occupation.value(i),
                "company": col_company.value(i),
                "country": col_country.value(i),
                "city": col_city.value(i),
                "street": col_street.value(i),
                "phone": col_phone.value(i),
                "salary": col_salary.value(i),
                "bonus": col_bonus.value(i),
                "currency": col_currency.value(i),
                "department": col_department.value(i),
                "join_date": col_join_date.value(i),
                "is_active": col_is_active.value(i),
                "score": col_score.value(i),
                "rating": col_rating.value(i),
                "category": col_category.value(i),
                "status": col_status.value(i),
                "priority": col_priority.value(i),
                "description": col_description.value(i),
                "notes": col_notes.value(i),
                "created_at": col_created_at.value(i),
                "updated_at": col_updated_at.value(i),
                "ip_address": col_ip_address.value(i),
                "user_agent": col_user_agent.value(i),
            });

            let pretty_str = serde_json::to_string_pretty(&obj)?;
            // Indent 2 spaces for array element
            for (idx, line) in pretty_str.lines().enumerate() {
                if idx > 0 {
                    writer.write_all(b"\n")?;
                }
                writer.write_all(b"  ")?;
                writer.write_all(line.as_bytes())?;
            }
        }

        progress.add_rows(n as u64);
        if let Ok(md) = std::fs::metadata(path) {
            progress.set_bytes(md.len());
        }
    }

    writer.write_all(b"\n]\n")?;
    writer.flush()?;
    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}
