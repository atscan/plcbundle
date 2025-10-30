use std::env;
use std::fs::File;
use std::io::{self, BufRead, BufReader, Write};
use std::time::Instant;
use sonic_rs::{Deserialize, from_str, JsonValueTrait, Value};

#[derive(Deserialize)]
struct Operation {
    did: String,
    cid: String,
    operation: Value,
}

fn detect(op: &Operation) -> Vec<String> {
    let mut labels = Vec::new();
    
    if op.did.starts_with("did:plc:aa") {
        labels.push("test".to_string());
    }
    
    if let Some(sig) = op.operation.get("sig") {
        eprintln!("{}", sig);
    }
    
    labels
}

fn main() -> io::Result<()> {
    let args: Vec<String> = env::args().collect();
    let bundle_dir = args.get(1).map(|s| s.as_str()).unwrap_or("./");
    let start_bundle: usize = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(1);
    let end_bundle: usize = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(100);
    
    eprintln!("Processing bundles {}-{} from {}", start_bundle, end_bundle, bundle_dir);
    eprintln!();
    
    let stdout = io::stdout();
    let mut writer = io::BufWriter::with_capacity(512 * 1024, stdout.lock());
    writeln!(writer, "bundle,position,cid,size,confidence,labels")?;
    
    let mut total_ops = 0;
    let mut match_count = 0;
    let mut total_bytes: u64 = 0;
    let mut matched_bytes: u64 = 0;
    let start_time = Instant::now();
    
    for bundle_num in start_bundle..=end_bundle {
        let bundle_file = format!("{}/{:06}.jsonl.zst", bundle_dir, bundle_num);
        
        let file = match File::open(&bundle_file) {
            Ok(f) => f,
            Err(_) => continue,
        };
        
        let decoder = match zstd::Decoder::new(file) {
            Ok(d) => d,
            Err(_) => continue,
        };
        
        let reader = BufReader::new(decoder);
        
        for (position, line) in reader.lines().enumerate() {
            let line = match line {
                Ok(l) => l,
                Err(_) => continue,
            };
            
            if line.is_empty() {
                continue;
            }
            
            total_ops += 1;
            let op_size = line.len();
            total_bytes += op_size as u64;
            
            let op: Operation = match from_str(&line) {
                Ok(o) => o,
                Err(_) => continue,
            };
            
            let labels = detect(&op);
            
            if !labels.is_empty() {
                match_count += 1;
                matched_bytes += op_size as u64;
                
                let cid_short = if op.cid.len() > 4 {
                    &op.cid[op.cid.len()-4..]
                } else {
                    &op.cid
                };
                
                writeln!(writer, "{},{},{},{},0.95,{}", 
                    bundle_num, position, cid_short, op_size, labels.join(";"))?;
            }
        }
        
        if bundle_num % 10 == 0 {
            let elapsed = start_time.elapsed().as_secs_f64();
            let ops_per_sec = total_ops as f64 / elapsed;
            eprint!("Processed {}/{} bundles | {} ops | {:.0} ops/sec\r", 
                bundle_num, end_bundle, total_ops, ops_per_sec);
        }
    }
    
    let elapsed = start_time.elapsed().as_secs_f64();
    writer.flush()?;
    
    eprintln!("\n\n✓ Detection complete");
    eprintln!("  Total operations:   {}", total_ops);
    eprintln!("  Matches found:      {} ({:.2}%)", match_count, 
        match_count as f64 / total_ops as f64 * 100.0);
    eprintln!("  Total size:         {:.1} MB", total_bytes as f64 / 1e6);
    eprintln!("  Matched size:       {:.1} MB ({:.2}%)", 
        matched_bytes as f64 / 1e6, 
        matched_bytes as f64 / total_bytes as f64 * 100.0);
    eprintln!();
    eprintln!("  Time elapsed:       {:.2}s", elapsed);
    eprintln!("  Throughput:         {:.0} ops/sec", total_ops as f64 / elapsed);
    eprintln!("  Speed:              {:.1} MB/sec", total_bytes as f64 / elapsed / 1e6);
    
    Ok(())
}
