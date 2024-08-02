use std::fs::File;
use std::io::{Read, BufReader};
use std::collections::HashMap;
use std::time::Instant;
use std::thread;

fn main() {
    let start_time = Instant::now();

    let file = File::open("./measurements.txt").expect("Failed to open file");
    let mut reader = BufReader::new(file);
    let mut buffer = Vec::with_capacity(100 * 1024 * 1024); // 100MB buffer
    let mut city_data: HashMap<String, (f64, f64, f64, u32)> = HashMap::new();

    let reading_start = Instant::now();
    while reader.read_to_end(&mut buffer).expect("Failed to read") > 0 {
        
        let num_threads = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4);
        let chunk_size = buffer.len() / num_threads; // Divide the buffer into num_threads parts
        let mut handles = vec![];

        for chunk in buffer.chunks(chunk_size) {
            let chunk = chunk.to_vec(); // Clone the chunk for the thread
            let handle = thread::spawn(move || {
                let mut local_data = HashMap::new();
                process_buffer(&chunk, &mut local_data);
                local_data
            });
            handles.push(handle);
        }

        for handle in handles {
            let local_data = handle.join().unwrap();
            merge_data(&mut city_data, local_data);
        }

        buffer.clear();
    }
    let reading_duration = reading_start.elapsed();

    let sorting_start = Instant::now();
    let mut sorted_cities: Vec<_> = city_data.into_iter().collect();
    sorted_cities.sort_by(|a, b| a.0.cmp(&b.0));
    let sorting_duration = sorting_start.elapsed();

    let formatting_start = Instant::now();
    let result = sorted_cities.iter()
        .map(|(city, (min, sum, max, count))| {
            let mean = sum / *count as f64;
            format!("{}={:.1}/{:.1}/{:.1}", city, min, mean, max)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let formatting_duration = formatting_start.elapsed();

    let total_duration = start_time.elapsed();

    println!("Result: {}", result);
    println!("\nTiming Information:");
    println!("Reading and processing: {:?}", reading_duration);
    println!("Sorting: {:?}", sorting_duration);
    println!("Formatting: {:?}", formatting_duration);
    println!("Total time: {:?}", total_duration);
}

fn process_buffer(buffer: &[u8], city_data: &mut HashMap<String, (f64, f64, f64, u32)>) {
    let mut start = 0;
    for (end, &byte) in buffer.iter().enumerate() {
        if byte == b'\n' {
            if let Ok(line) = std::str::from_utf8(&buffer[start..end]) {
                if let Some((location, temperature)) = line.split_once(';') {
                    if let Ok(temp) = temperature.parse::<f64>() {
                        let entry = city_data.entry(location.to_string()).or_insert((f64::MAX, 0.0, f64::MIN, 0));
                        entry.0 = entry.0.min(temp);  // min
                        entry.1 += temp;  // sum for mean
                        entry.2 = entry.2.max(temp);  // max
                        entry.3 += 1;  // count for mean
                    }
                }
            }
            start = end + 1;
        }
    }
}

fn merge_data(main_data: &mut HashMap<String, (f64, f64, f64, u32)>, local_data: HashMap<String, (f64, f64, f64, u32)>) {
    for (city, (min, sum, max, count)) in local_data {
        let entry = main_data.entry(city).or_insert((f64::MAX, 0.0, f64::MIN, 0));
        entry.0 = entry.0.min(min);
        entry.1 += sum;
        entry.2 = entry.2.max(max);
        entry.3 += count;
    }
}