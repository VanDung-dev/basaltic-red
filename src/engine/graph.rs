use arrow_schema::Schema;
use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

use crate::engine::MatrixEngine;
use crate::error::BazanError;

impl MatrixEngine {
    /// Auto-detect relationships & generate Mermaid ER Diagram
    pub fn generate_er_graph(&self, path: &str, output_path: Option<&str>) -> Result<String, BazanError> {
        let input_path = Path::new(path);
        let mut schemas: Vec<(String, Arc<Schema>)> = Vec::new();
        let mut seen_tables: HashSet<String> = HashSet::new();

        if input_path.is_dir() {
            for entry in fs::read_dir(input_path)? {
                let entry = entry?;
                let p = entry.path();
                if p.is_file() {
                    let name = p.file_stem().and_then(|s| s.to_str()).unwrap_or("Table").to_string();
                    if !seen_tables.contains(&name) {
                        if let Ok(batch) = self.slice_rows_native(p.to_str().unwrap(), 0, 1) {
                            seen_tables.insert(name.clone());
                            schemas.push((name, batch.schema()));
                        }
                    }
                }
            }
        } else if input_path.is_file() {
            let batch = self.slice_rows_native(path, 0, 1)?;
            let name = input_path.file_stem().and_then(|s| s.to_str()).unwrap_or("Table").to_string();
            schemas.push((name, batch.schema()));
        }

        // Sort tables by name for clean visual output
        schemas.sort_by(|a, b| a.0.cmp(&b.0));

        let mut mermaid = String::from("```mermaid\nerDiagram\n");

        for (table_name, schema) in &schemas {
            mermaid.push_str(&format!("    {} {{\n", table_name));
            for field in schema.fields() {
                let dtype = format!("{:?}", field.data_type()).to_lowercase();
                let f_name = field.name();

                let key_tag = if f_name == "id" || f_name == &format!("{}_id", table_name) {
                    "PK"
                } else if f_name.ends_with("_id") {
                    "FK"
                } else {
                    ""
                };

                mermaid.push_str(&format!("        {} {} {}\n", dtype, f_name, key_tag));
            }
            mermaid.push_str("    }\n");
        }

        // Auto-detect foreign key relationship links (e.g. orders.user_id -> users.id)
        for i in 0..schemas.len() {
            for j in 0..schemas.len() {
                if i != j {
                    let (t1, s1) = &schemas[i];
                    let (t2, _) = &schemas[j];

                    // Check standard foreign key conventions: user_id -> users / user
                    let fk_singular = format!("{}_id", t2.trim_end_matches('s').to_lowercase());
                    let fk_plural = format!("{}_id", t2.to_lowercase());

                    let is_linked = s1.fields().iter().any(|f| {
                        let name_lower = f.name().to_lowercase();
                        name_lower == fk_singular || name_lower == fk_plural
                    });

                    if is_linked {
                        mermaid.push_str(&format!("    {} }}|--|| {} : \"{}\"\n", t1, t2, fk_plural));
                    }
                }
            }
        }

        mermaid.push_str("```\n");

        if let Some(out) = output_path {
            fs::write(out, &mermaid)?;
        }

        Ok(mermaid)
    }
}
