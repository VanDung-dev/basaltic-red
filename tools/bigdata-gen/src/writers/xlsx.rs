use anyhow::Result;
use arrow::array::*;
use rust_xlsxwriter::*;

use crate::gen::{self, chunk_iter};
use crate::progress::ProgressItem;

pub fn write_xlsx(
    path: &str,
    seed: u64,
    total: u64,
    cols: usize,
    progress: &ProgressItem,
) -> Result<()> {
    const MAX_XLSX_ROWS: u32 = 1_048_576;
    let bold = Format::new().set_bold();

    let mut workbook = Workbook::new();
    let mut row: u32 = 1;

    let sch = gen::schema(cols);
    let col_names: Vec<String> = sch.fields().iter().map(|f| f.name().clone()).collect();

    let sheet = workbook.add_worksheet();
    for (col, name) in col_names.iter().enumerate() {
        sheet.write_string_with_format(0, col as u16, name, &bold)?;
    }

    let mut accum_rows: u64 = 0;
    for batch in chunk_iter(seed, total, cols) {
        let n = batch.num_rows();

        let col_id = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let col_uuid = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_first_name = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_last_name = batch
            .column(3)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_email = batch
            .column(4)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_age = batch
            .column(5)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col_gender = batch
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_occupation = batch
            .column(7)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_company = batch
            .column(8)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_country = batch
            .column(9)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_city = batch
            .column(10)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_street = batch
            .column(11)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_phone = batch
            .column(12)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_salary = batch
            .column(13)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let col_bonus = batch
            .column(14)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let col_currency = batch
            .column(15)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_department = batch
            .column(16)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_join_date = batch
            .column(17)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_is_active = batch
            .column(18)
            .as_any()
            .downcast_ref::<BooleanArray>()
            .unwrap();
        let col_score = batch
            .column(19)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let col_rating = batch
            .column(20)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col_category = batch
            .column(21)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_status = batch
            .column(22)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_priority = batch
            .column(23)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_description = batch
            .column(24)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_notes = batch
            .column(25)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_created_at = batch
            .column(26)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_updated_at = batch
            .column(27)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_ip_address = batch
            .column(28)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let col_user_agent = batch
            .column(29)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        for i in 0..n {
            if row >= MAX_XLSX_ROWS {
                let sheet = workbook.add_worksheet();
                for (col, name) in gen::COLUMN_NAMES.iter().enumerate() {
                    sheet.write_string_with_format(0, col as u16, *name, &bold)?;
                }
                row = 1;
            }

            let last_idx = workbook.worksheets().len() - 1;
            let sheet = workbook.worksheet_from_index(last_idx)?;
            sheet.write_number(row, 0, col_id.value(i) as f64)?;
            sheet.write_string(row, 1, col_uuid.value(i))?;
            sheet.write_string(row, 2, col_first_name.value(i))?;
            sheet.write_string(row, 3, col_last_name.value(i))?;
            sheet.write_string(row, 4, col_email.value(i))?;
            sheet.write_number(row, 5, col_age.value(i) as f64)?;
            sheet.write_string(row, 6, col_gender.value(i))?;
            sheet.write_string(row, 7, col_occupation.value(i))?;
            sheet.write_string(row, 8, col_company.value(i))?;
            sheet.write_string(row, 9, col_country.value(i))?;
            sheet.write_string(row, 10, col_city.value(i))?;
            sheet.write_string(row, 11, col_street.value(i))?;
            sheet.write_string(row, 12, col_phone.value(i))?;
            sheet.write_number(row, 13, col_salary.value(i))?;
            sheet.write_number(row, 14, col_bonus.value(i))?;
            sheet.write_string(row, 15, col_currency.value(i))?;
            sheet.write_string(row, 16, col_department.value(i))?;
            sheet.write_string(row, 17, col_join_date.value(i))?;
            sheet.write_boolean(row, 18, col_is_active.value(i))?;
            sheet.write_number(row, 19, col_score.value(i))?;
            sheet.write_number(row, 20, col_rating.value(i) as f64)?;
            sheet.write_string(row, 21, col_category.value(i))?;
            sheet.write_string(row, 22, col_status.value(i))?;
            sheet.write_string(row, 23, col_priority.value(i))?;
            sheet.write_string(row, 24, col_description.value(i))?;
            sheet.write_string(row, 25, col_notes.value(i))?;
            sheet.write_string(row, 26, col_created_at.value(i))?;
            sheet.write_string(row, 27, col_updated_at.value(i))?;
            sheet.write_string(row, 28, col_ip_address.value(i))?;
            sheet.write_string(row, 29, col_user_agent.value(i))?;

            row += 1;
        }

        accum_rows += n as u64;
        progress.add_rows(n as u64);
        // ponytail: ~200 bytes per row estimated in compressed XLSX
        progress.set_bytes(accum_rows * 200);
    }

    let file = std::fs::File::create(path)?;
    workbook.save_to_writer(file)?;

    if let Ok(md) = std::fs::metadata(path) {
        progress.set_bytes(md.len());
    }
    Ok(())
}
