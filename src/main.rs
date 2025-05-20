use std::{collections::HashMap, fs, io, path::Path};
use append_only_logs::Operation;
use atomic_writer::atomic_write;
use append_only_logs::{LogWriter, LogReader};
use log::{info, error};

fn main() {
    // Initialize the logger
    env_logger::init();

    cleanup_files().expect("Cleanup failed");
    
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

    let log_path = Path::new("operations.log");
    // let mut state: HashMap<String, Vec<u8>> = HashMap::new();

    let ops = vec![
        Operation::Set { key: "a".into(), value: b"1".to_vec() },
        Operation::Set { key: "b".into(), value: b"2".to_vec() },
        Operation::Set { key: "c".into(), value: b"3".to_vec() },
        Operation::Delete { key: "b".into() },
    ];

    match process_operations(log_path, &ops) {
        Ok(_) => {
            info!("Operations processed successfully");
            println!("Operations processed successfully");
        }
        Err(e) => {
            error!("Failed to process operations: {}", e);
            eprintln!("Failed to process operations: {}", e);
        }
    }

        // Recover state from log
        match recover_state(log_path) {
            Ok(recovered) => {
                println!("[Success] Recovered state:");
                for (k, v) in &recovered {
                    println!("- {}: {:?}", k, v);
                }
                // state = recovered;
            }
            Err(e) => eprintln!("[Error] Recovery failed: {}", e),
        }
}

fn process_operations(path: &Path, ops: &[Operation]) -> io::Result<()> {
    let mut log = LogWriter::open(path)?;
    for op in ops {
        log.append(op)?;
        // Simulate some processing delay
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    Ok(())
}

fn recover_state(path: &Path) -> io::Result<HashMap<String, Vec<u8>>> {
    let mut reader = LogReader::open(path)?;
    let mut state = HashMap::new();
    
    for op in reader.read_entries()? {
        match op {
            Operation::Set { key, value } => {
                state.insert(key, value);
            }
            Operation::Delete { key } => {
                state.remove(&key);
            }
        }
    }
    Ok(state)
}

fn cleanup_files() -> io::Result<()> {
    let files = ["atomic_data.txt", "operations.log", "crash.log"];
    for f in files {
        let _ = fs::remove_file(f);
    }

    Ok(())
}