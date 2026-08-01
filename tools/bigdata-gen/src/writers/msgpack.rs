use std::fs::File;
use std::io::BufWriter;
use anyhow::Result;
use arrow::array::*;

use crate::gen::chunk_iter;
use crate::progress::ProgressItem;

pub fn write_msgpack(path: &str, seed: u64, total: u64, cols: usize, progress: &ProgressItem) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);

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
            // Write MessagePack map of 30 fields
            let map: std::collections::BTreeMap<&str, rmpv::Value> = [
                ("id", rmpv::Value::Integer(col_id.value(i).into())),
                ("uuid", rmpv::Value::String(col_uuid.value(i).into())),
                ("first_name", rmpv::Value::String(col_first_name.value(i).into())),
                ("last_name", rmpv::Value::String(col_last_name.value(i).into())),
                ("email", rmpv::Value::String(col_email.value(i).into())),
                ("age", rmpv::Value::Integer(col_age.value(i).into())),
                ("gender", rmpv::Value::String(col_gender.value(i).into())),
                ("occupation", rmpv::Value::String(col_occupation.value(i).into())),
                ("company", rmpv::Value::String(col_company.value(i).into())),
                ("country", rmpv::Value::String(col_country.value(i).into())),
                ("city", rmpv::Value::String(col_city.value(i).into())),
                ("street", rmpv::Value::String(col_street.value(i).into())),
                ("phone", rmpv::Value::String(col_phone.value(i).into())),
                ("salary", rmpv::Value::F64(col_salary.value(i))),
                ("bonus", rmpv::Value::F64(col_bonus.value(i))),
                ("currency", rmpv::Value::String(col_currency.value(i).into())),
                ("department", rmpv::Value::String(col_department.value(i).into())),
                ("join_date", rmpv::Value::String(col_join_date.value(i).into())),
                ("is_active", rmpv::Value::Boolean(col_is_active.value(i))),
                ("score", rmpv::Value::F64(col_score.value(i))),
                ("rating", rmpv::Value::Integer(col_rating.value(i).into())),
                ("category", rmpv::Value::String(col_category.value(i).into())),
                ("status", rmpv::Value::String(col_status.value(i).into())),
                ("priority", rmpv::Value::String(col_priority.value(i).into())),
                ("description", rmpv::Value::String(col_description.value(i).into())),
                ("notes", rmpv::Value::String(col_notes.value(i).into())),
                ("created_at", rmpv::Value::String(col_created_at.value(i).into())),
                ("updated_at", rmpv::Value::String(col_updated_at.value(i).into())),
                ("ip_address", rmpv::Value::String(col_ip_address.value(i).into())),
                ("user_agent", rmpv::Value::String(col_user_agent.value(i).into())),
            ].into_iter().collect();

            rmp_serde::encode::write_named(&mut writer, &map)?;
        }

        progress.add_rows(n as u64);
        if let Ok(md) = std::fs::metadata(path) {
            progress.set_bytes(md.len());
        }
    }

    use std::io::Write;
    writer.flush()?;
    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}
