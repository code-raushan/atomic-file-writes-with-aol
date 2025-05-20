use std::{fs::{self, File}, io::{self, Write}, path::{Path, PathBuf}};
use uuid::Uuid;


pub fn atomic_write(path: &Path, data: &[u8]) -> io::Result<()> {
    let temp_path = generate_temp_path(path)?;

    let mut temp_file = File::create(&temp_path)?;

    temp_file.write_all(data)?;
    // fsync 
    temp_file.sync_all()?;

    // Atomic replace -> by default, the file will be replaced atomically
    fs::rename(temp_path, path)?;

    Ok(())
}

fn generate_temp_path(target: &Path) -> io::Result<PathBuf> {
    let dir = target.parent().ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "Target Path is invalid"))?;

    Ok(dir.join(format!(".tmp.{}", Uuid::new_v4())))
}

