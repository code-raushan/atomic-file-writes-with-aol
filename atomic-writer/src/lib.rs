use std::{fs::{self, File}, io::{self, Write}, path::{Path, PathBuf}};
use uuid::Uuid;
use log::{info, warn, error, debug};

pub fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    info!("Starting atomic write to path: {:?}", path);
    
    // Create parent directory if it doesn't exist
    if let Some(parent) = path.parent() {
        debug!("Creating parent directory if it doesn't exist: {:?}", parent);
        fs::create_dir_all(parent)?;
    }

    let temp_path = generate_temp_path(path)?;
    debug!("Generated temporary path: {:?}", temp_path);

    info!("Creating temporary file");
    let mut temp_file = File::create(&temp_path)?;

    debug!("Writing {} bytes to temporary file", data.len());
    temp_file.write_all(data)?;
    
    info!("Syncing temporary file to disk");
    temp_file.sync_all()?;

    info!("Performing atomic rename from {:?} to {:?}", temp_path, path);
    fs::rename(temp_path, path)?;

    info!("Syncing parent directory metadata");
    sync_parent_dir(path)?;

    info!("Atomic write completed successfully");
    Ok(())
}

fn generate_temp_path(target: &Path) -> io::Result<PathBuf> {
    debug!("Generating temporary path for target: {:?}", target);
    let dir = target.parent().ok_or_else(|| {
        error!("Invalid target path - no parent directory: {:?}", target);
        io::Error::new(io::ErrorKind::InvalidInput, "Target Path is invalid")
    })?;

    let temp_path = dir.join(format!(".tmp.{}", Uuid::new_v4()));
    debug!("Generated temporary path: {:?}", temp_path);
    Ok(temp_path)
}

fn sync_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        // Skip syncing if parent is root/empty directory
        if parent.as_os_str().is_empty() {
            debug!("Skipping parent directory sync for root directory");
            return Ok(());
        }
        
        debug!("Syncing parent directory: {:?}", parent);
        match File::open(parent) {
            Ok(file) => {
                file.sync_all()?;
                debug!("Successfully synced parent directory");
            }
            Err(e) => {
                warn!("Failed to sync parent directory: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

