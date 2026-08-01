use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

const BAR_WIDTH: usize = 10;

pub struct ProgressItem {
    pub name: String,
    pub rows: AtomicU64,
    pub total: u64,
    pub bytes: AtomicU64,
    pub error: AtomicBool,
    pub finished: AtomicBool,
    pub running: AtomicBool,
    pub started: Mutex<Instant>,
    pub elapsed_us: AtomicU64,
}

impl ProgressItem {
    pub fn new(name: &str, total: u64) -> Self {
        Self {
            name: name.to_string(),
            rows: AtomicU64::new(0),
            total,
            bytes: AtomicU64::new(0),
            error: AtomicBool::new(false),
            finished: AtomicBool::new(false),
            running: AtomicBool::new(false),
            started: Mutex::new(Instant::now()),
            elapsed_us: AtomicU64::new(0),
        }
    }

    pub fn reset_started(&self) {
        *self.started.lock().unwrap() = Instant::now();
        self.running.store(true, Ordering::Relaxed);
    }

    pub fn elapsed_secs(&self) -> f64 {
        if !self.running.load(Ordering::Relaxed) {
            0.0
        } else {
            self.started.lock().unwrap().elapsed().as_secs_f64()
        }
    }

    pub fn add_rows(&self, n: u64) {
        self.rows.fetch_add(n, Ordering::Relaxed);
    }

    pub fn set_bytes(&self, n: u64) {
        self.bytes.store(n, Ordering::Relaxed);
    }

    pub fn mark_finished(&self) {
        let elapsed = self.started.lock().unwrap().elapsed();
        self.elapsed_us.store(elapsed.as_micros() as u64, Ordering::Relaxed);
        self.running.store(false, Ordering::Relaxed);
    }

    pub fn actual_elapsed_secs(&self) -> f64 {
        self.elapsed_us.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }

    fn bar(&self, pct: f64) -> String {
        let filled = (pct * BAR_WIDTH as f64).round() as usize;
        let empty = BAR_WIDTH.saturating_sub(filled);
        format!(
            "{}{}",
            "█".repeat(filled),
            "░".repeat(empty)
        )
    }
}

pub struct Dashboard {
    items: Vec<Arc<ProgressItem>>,
    cols: usize,
    #[allow(dead_code)]
    started: Instant,
    lines: usize,
}

impl Dashboard {
    pub fn new(items: Vec<Arc<ProgressItem>>, cols: usize) -> Self {
        let lines = 5 + items.len();
        Self { items, cols, started: Instant::now(), lines }
    }


    pub fn render(&self) {
        let total_rows: u64 = self.items.iter().map(|p| p.rows.load(Ordering::Relaxed)).sum();

        let mut out = String::new();
        out.push_str(&format!(
            " BigData Generator           {:>12} rows × {} cols\n",
            format_num(self.items[0].total),
            self.cols
        ));

        out.push_str(&"─".repeat(68));
        out.push('\n');
        out.push_str(&format!(
            " {:<8} {:<12} {:>12} {:>10} {:>10} {:>8} {:>4}",
            "Format", "Progress", "Rows", "Size", "Speed", "Time", "Status"
        ));
        out.push('\n');

        for item in &self.items {
            let rows = item.rows.load(Ordering::Relaxed);
            let bytes = item.bytes.load(Ordering::Relaxed);
            let cell_elapsed = if item.finished.load(Ordering::Relaxed) {
                item.actual_elapsed_secs()
            } else {
                item.elapsed_secs()
            };
            let pct = if item.total > 0 {
                rows as f64 / item.total as f64
            } else {
                0.0
            };
            let speed = if cell_elapsed > 0.0 {
                (rows as f64 / cell_elapsed) as u64
            } else {
                0
            };
            let status = if item.finished.load(Ordering::Relaxed) {
                if item.error.load(Ordering::Relaxed) { "✗" } else { "✓" }
            } else if item.running.load(Ordering::Relaxed) {
                "⏳"
            } else {
                "⋯"
            };

            let row_str = format!("{}/{}", format_short(rows), format_short(item.total));
            let speed_str = format!("{}/s", format_short(speed));

            out.push_str(&format!(
                " {:<8} {:<12} {:>12} {:>10} {:>10} {:>8.1}s  {}",
                item.name,
                item.bar(pct.min(1.0)),
                row_str,
                format_bytes(bytes),
                speed_str,
                cell_elapsed,
                status,
            ));
            out.push('\n');
        }

        out.push_str(&"─".repeat(68));
        out.push('\n');
        out.push_str(&format!(" Total: {} rows generated", format_num(total_rows)));

        print!("\r\x1b[{}A\x1b[J{}", self.lines, out);
        use std::io::{Write, stdout};
        stdout().flush().ok();
    }


    pub fn initial_render(&self) {
        for _ in 0..self.lines {
            println!();
        }
        print!("\x1b[{}A", self.lines);
        self.render();
    }
}

pub fn format_num(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.1}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}K", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_short(n: u64) -> String {
    format_num(n)
}

pub fn format_bytes(bytes: u64) -> String {
    if bytes >= 1_000_000_000 {
        format!("{:.2}GB", bytes as f64 / 1_000_000_000.0)
    } else if bytes >= 1_000_000 {
        format!("{:.2}MB", bytes as f64 / 1_000_000.0)
    } else if bytes >= 1_000 {
        format!("{:.1}KB", bytes as f64 / 1_000.0)
    } else {
        format!("{}B", bytes)
    }
}


