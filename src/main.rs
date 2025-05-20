use std::{fs, path::Path};
use atomic_writer::atomic_write;

fn main() {
    print!("--- Atomic Write Test ---");
    let atomic_path = Path::new("atomic_data.txt");
    let atomic_data = b"Testing atomic writes";

    match atomic_write(atomic_path, atomic_data) {
        Ok(_) => {
            println!("Atomic write successfully completed");

            match fs::read(atomic_path) {
                Ok(data) => {
                    assert_eq!(data, atomic_data, "Written data does not match the original data");
                }
                Err(e) => {
                    eprintln!("Failed to read atomic file: {}", e);
                }
            }
        }
        Err(e) => {
            eprintln!("Atomic write failed: {}", e);
        }
    }

    println!("--- Append-Only Log Test ---");
    
}