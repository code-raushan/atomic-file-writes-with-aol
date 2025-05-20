use std::{fs, path::Path};
use atomic_writer::atomic_write;
use log::{info, error};

fn main() {
    // Initialize the logger
    env_logger::init();
    
    info!("Starting atomic file write test");
    print!("--- Atomic Write Test ---");
    let atomic_path = Path::new("atomic_data.txt");
    let atomic_data = b"Testing atomic writes";

    match atomic_write(atomic_path, atomic_data) {
        Ok(_) => {
            info!("Atomic write successfully completed");
            println!("Atomic write successfully completed");

            match fs::read(atomic_path) {
                Ok(data) => {
                    assert_eq!(data, atomic_data, "Written data does not match the original data");
                    info!("Successfully verified written data matches original data");
                }
                Err(e) => {
                    error!("Failed to read atomic file: {}", e);
                    eprintln!("Failed to read atomic file: {}", e);
                }
            }
        }
        Err(e) => {
            error!("Atomic write failed: {}", e);
            eprintln!("Atomic write failed: {}", e);
        }
    }

    println!("--- Append-Only Log Test ---");
    
}