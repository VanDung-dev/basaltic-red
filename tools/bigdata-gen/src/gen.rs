use arrow::array::*;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::sync::Arc;

const CHUNK_SIZE: usize = 100_000;

const FIRST: &[&str] = &["James","Mary","John","Patricia","Robert","Jennifer","Michael","Linda","David","Elizabeth","William","Barbara","Richard","Susan","Joseph","Jessica","Thomas","Sarah","Christopher","Karen"];
const LAST: &[&str] = &["Smith","Johnson","Williams","Brown","Jones","Garcia","Miller","Davis","Rodriguez","Martinez","Hernandez","Lopez","Gonzalez","Wilson","Anderson","Thomas","Taylor","Moore","Jackson","Martin"];
const OCC: &[&str] = &["Engineer","Teacher","Doctor","Nurse","Manager","Analyst","Developer","Designer","Accountant","Consultant","Architect","Scientist","Writer","Artist","Technician","Supervisor","Coordinator","Director"];
const COMP: &[&str] = &["Acme Corp","Globex","Initech","Umbrella","Cyberdyne","Wonka Industries","Stark Industries","Wayne Enterprises","Oscorp","Massive Dynamic","Hooli","Dunder Mifflin","Sterling Cooper"];
const CURR: &[&str] = &["USD","EUR","GBP","JPY","CNY","AUD","CAD","CHF","HKD","SGD"];
const DEPT: &[&str] = &["Engineering","Sales","Marketing","HR","Finance","Legal","Operations","R&D","Support","Admin"];
const CAT: &[&str] = &["A","B","C","D","E"];
const STAT: &[&str] = &["active","inactive","pending","suspended","archived"];
const PRIO: &[&str] = &["low","medium","high","critical"];
const CITY: &[&str] = &["New York","Los Angeles","Chicago","Houston","Phoenix","Philadelphia","San Antonio","San Diego","Dallas","San Jose","Austin","Jacksonville","Fort Worth","Columbus","Charlotte"];
const CTRY: &[&str] = &["USA","Canada","UK","Germany","France","Australia","Japan","Brazil","India","Mexico"];
const ST: &[&str] = &["Main St","Oak Ave","Elm St","Park Rd","Broadway","Lake Dr","Hill St","Cedar Ln","Pine Ave","Maple Dr"];
const UA: &[&str] = &[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 Safari/17.2",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/119.0.0.0",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_2 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148",
];

pub const COLUMN_NAMES: &[&str] = &[
    "id", "uuid", "first_name", "last_name", "email", "age", "gender", "occupation",
    "company", "country", "city", "street", "phone", "salary", "bonus", "currency",
    "department", "join_date", "is_active", "score", "rating", "category", "status",
    "priority", "description", "notes", "created_at", "updated_at", "ip_address", "user_agent",
];

pub fn schema(target_cols: usize) -> SchemaRef {
    let base_fields = vec![
        Field::new("id", DataType::Int64, false),
        Field::new("uuid", DataType::Utf8, false),
        Field::new("first_name", DataType::Utf8, false),
        Field::new("last_name", DataType::Utf8, false),
        Field::new("email", DataType::Utf8, false),
        Field::new("age", DataType::Int32, false),
        Field::new("gender", DataType::Utf8, false),
        Field::new("occupation", DataType::Utf8, false),
        Field::new("company", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, false),
        Field::new("city", DataType::Utf8, false),
        Field::new("street", DataType::Utf8, false),
        Field::new("phone", DataType::Utf8, false),
        Field::new("salary", DataType::Float64, false),
        Field::new("bonus", DataType::Float64, false),
        Field::new("currency", DataType::Utf8, false),
        Field::new("department", DataType::Utf8, false),
        Field::new("join_date", DataType::Utf8, false),
        Field::new("is_active", DataType::Boolean, false),
        Field::new("score", DataType::Float64, false),
        Field::new("rating", DataType::Int32, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("status", DataType::Utf8, false),
        Field::new("priority", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
        Field::new("notes", DataType::Utf8, false),
        Field::new("created_at", DataType::Utf8, false),
        Field::new("updated_at", DataType::Utf8, false),
        Field::new("ip_address", DataType::Utf8, false),
        Field::new("user_agent", DataType::Utf8, false),
    ];

    let mut fields = Vec::with_capacity(target_cols);
    for i in 0..target_cols {
        let base_f = &base_fields[i % base_fields.len()];
        let name = if i < base_fields.len() {
            base_f.name().clone()
        } else {
            format!("{}_{}", base_f.name(), (i / base_fields.len()) + 1)
        };
        fields.push(Field::new(name, base_f.data_type().clone(), base_f.is_nullable()));
    }
    Arc::new(Schema::new(fields))
}

fn pick<'a>(pool: &[&'a str], rng: &mut ChaCha8Rng) -> &'a str {
    pool[rng.gen_range(0..pool.len())]
}

fn uuid_str(rng: &mut ChaCha8Rng) -> String {
    let b: [u8; 16] = rng.gen();
    let hex = "0123456789abcdef".as_bytes();
    let mut s = String::with_capacity(36);
    for i in 0..16 {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            s.push('-');
        }
        s.push(hex[(b[i] >> 4) as usize] as char);
        s.push(hex[(b[i] & 0x0f) as usize] as char);
    }
    s
}

fn rand_date(rng: &mut ChaCha8Rng, start_epoch_days: i64, end_epoch_days: i64) -> String {
    let days = rng.gen_range(start_epoch_days..=end_epoch_days);
    let y = 1970 + (days + 10957) / 365;

    let m = 1 + (days % 365) / 31;
    let d = 1 + (days % 28);
    format!("{y:04}-{m:02}-{d:02}")
}

pub fn generate_chunk(rng: &mut ChaCha8Rng, offset: i64, n: usize, target_cols: usize) -> RecordBatch {
    let full_batch_30 = generate_base_30_chunk(rng, offset, n);
    let num_base_cols = 30;

    if target_cols == num_base_cols {
        return full_batch_30;
    }

    let mut cols: Vec<ArrayRef> = Vec::with_capacity(target_cols);
    for i in 0..target_cols {
        let base_col = full_batch_30.column(i % num_base_cols);
        cols.push(base_col.clone());
    }

    let sch = schema(target_cols);
    RecordBatch::try_new(sch, cols).expect("generate_chunk: RecordBatch creation failed")
}

const NUM_BASE_COLS: usize = 30;

fn generate_base_30_chunk(rng: &mut ChaCha8Rng, offset: i64, n: usize) -> RecordBatch {

    let mut id_b = Int64Builder::with_capacity(n);
    let mut uuid_b = StringBuilder::with_capacity(n, n * 37);
    let mut first_b = StringBuilder::with_capacity(n, n * 10);
    let mut last_b = StringBuilder::with_capacity(n, n * 10);
    let mut email_b = StringBuilder::with_capacity(n, n * 30);
    let mut age_b = Int32Builder::with_capacity(n);
    let mut gender_b = StringBuilder::with_capacity(n, n * 6);
    let mut occ_b = StringBuilder::with_capacity(n, n * 15);
    let mut comp_b = StringBuilder::with_capacity(n, n * 20);
    let mut ctry_b = StringBuilder::with_capacity(n, n * 10);
    let mut city_b = StringBuilder::with_capacity(n, n * 15);
    let mut street_b = StringBuilder::with_capacity(n, n * 20);
    let mut phone_b = StringBuilder::with_capacity(n, n * 16);
    let mut salary_b = Float64Builder::with_capacity(n);
    let mut bonus_b = Float64Builder::with_capacity(n);
    let mut curr_b = StringBuilder::with_capacity(n, n * 4);
    let mut dept_b = StringBuilder::with_capacity(n, n * 15);
    let mut jdate_b = StringBuilder::with_capacity(n, n * 11);
    let mut active_b = BooleanBuilder::with_capacity(n);
    let mut score_b = Float64Builder::with_capacity(n);
    let mut rating_b = Int32Builder::with_capacity(n);
    let mut cat_b = StringBuilder::with_capacity(n, n * 2);
    let mut stat_b = StringBuilder::with_capacity(n, n * 10);
    let mut prio_b = StringBuilder::with_capacity(n, n * 8);
    let mut desc_b = StringBuilder::with_capacity(n, n * 30);
    let mut notes_b = StringBuilder::with_capacity(n, n * 20);
    let mut cdate_b = StringBuilder::with_capacity(n, n * 11);
    let mut udate_b = StringBuilder::with_capacity(n, n * 11);
    let mut ip_b = StringBuilder::with_capacity(n, n * 16);
    let mut ua_b = StringBuilder::with_capacity(n, n * 80);

    for i in 0..n {
        let id = offset + i as i64;
        let first = pick(FIRST, rng);
        let last = pick(LAST, rng);

        id_b.append_value(id);
        uuid_b.append_value(&uuid_str(rng));
        first_b.append_value(first);
        last_b.append_value(last);
        email_b.append_value(&format!("{}.{}{}@example.com",
            first.to_lowercase(), last.to_lowercase(), rng.gen_range(1..999)));

        age_b.append_value(rng.gen_range(18..75));
        gender_b.append_value(pick(&["Male", "Female", "Other"], rng));
        occ_b.append_value(pick(OCC, rng));
        comp_b.append_value(pick(COMP, rng));
        ctry_b.append_value(pick(CTRY, rng));
        city_b.append_value(pick(CITY, rng));
        street_b.append_value(&format!("{} {}", rng.gen_range(1..9999), pick(ST, rng)));
        phone_b.append_value(&format!("+1-{:03}-{:03}-{:04}",
            rng.gen_range(200..999), rng.gen_range(100..999), rng.gen_range(1000..9999)));

        let salary: f64 = rng.gen_range(30_000.0..200_000.0);
        salary_b.append_value((salary * 100.0).round() / 100.0);
        let bonus: f64 = rng.gen_range(0.0..50_000.0);
        bonus_b.append_value((bonus * 100.0).round() / 100.0);

        curr_b.append_value(pick(CURR, rng));
        dept_b.append_value(pick(DEPT, rng));
        jdate_b.append_value(&rand_date(rng, 18262, 20087));

        active_b.append_value(rng.gen_bool(0.85));

        let score: f64 = rng.gen_range(0.0..100.0);
        score_b.append_value((score * 100.0).round() / 100.0);
        rating_b.append_value(rng.gen_range(1..6));
        cat_b.append_value(pick(CAT, rng));
        stat_b.append_value(pick(STAT, rng));
        prio_b.append_value(pick(PRIO, rng));
        desc_b.append_value(&format!("Sample description {}", id));
        notes_b.append_value(&format!("Note {}", rng.gen_range(1000..9999)));
        cdate_b.append_value(&rand_date(rng, 19358, 19903));
        udate_b.append_value(&rand_date(rng, 19723, 20087));
        ip_b.append_value(&format!("{}.{}.{}.{}",
            rng.gen_range(10..223), rng.gen_range(0..255),
            rng.gen_range(0..255), rng.gen_range(1..254)));
        ua_b.append_value(pick(UA, rng));
    }

    RecordBatch::try_new(
        schema(NUM_BASE_COLS),
        vec![

            Arc::new(id_b.finish()),
            Arc::new(uuid_b.finish()),
            Arc::new(first_b.finish()),
            Arc::new(last_b.finish()),
            Arc::new(email_b.finish()),
            Arc::new(age_b.finish()),
            Arc::new(gender_b.finish()),
            Arc::new(occ_b.finish()),
            Arc::new(comp_b.finish()),
            Arc::new(ctry_b.finish()),
            Arc::new(city_b.finish()),
            Arc::new(street_b.finish()),
            Arc::new(phone_b.finish()),
            Arc::new(salary_b.finish()),
            Arc::new(bonus_b.finish()),
            Arc::new(curr_b.finish()),
            Arc::new(dept_b.finish()),
            Arc::new(jdate_b.finish()),
            Arc::new(active_b.finish()),
            Arc::new(score_b.finish()),
            Arc::new(rating_b.finish()),
            Arc::new(cat_b.finish()),
            Arc::new(stat_b.finish()),
            Arc::new(prio_b.finish()),
            Arc::new(desc_b.finish()),
            Arc::new(notes_b.finish()),
            Arc::new(cdate_b.finish()),
            Arc::new(udate_b.finish()),
            Arc::new(ip_b.finish()),
            Arc::new(ua_b.finish()),
        ],
    )
    .expect("generate_chunk: RecordBatch creation failed")
}

pub fn chunk_iter(
    seed: u64,
    total: u64,
    target_cols: usize,
) -> impl Iterator<Item = RecordBatch> {
    let chunk = CHUNK_SIZE;
    let mut offset: i64 = 0;
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    std::iter::from_fn(move || {
        if offset as u64 >= total {
            return None;
        }
        let n = (total - offset as u64).min(chunk as u64) as usize;
        let batch = generate_chunk(&mut rng, offset, n, target_cols);
        offset += n as i64;
        Some(batch)
    })
}

