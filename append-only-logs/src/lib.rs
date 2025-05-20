use std::{fs::{File, OpenOptions}, io::{ self, Read, Write}, path::{Path, PathBuf}};
use bincode::{Encode, Decode};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Encode, Decode)]
pub enum Operation {
    Set { key: String, value: Vec<u8> },
    Delete { key: String }
}

pub struct LogWriter {
    file: File,
    path: PathBuf,
}

pub struct LogReader {
    file: File,
}

impl LogWriter {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref()
        .to_path_buf();

        let file = OpenOptions::new()
            .append(true)
            .create(true)
            .open(&path)?;

        Ok(Self { file, path })
    }

    pub fn append(&mut self, op: &Operation) -> io::Result<()> {
        let serialized_payload = bincode::encode_to_vec(op, bincode::config::standard()).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        let checksum = crc32fast::hash(&serialized_payload);
        let len = serialized_payload.len() as u32;

        // Write header
        self.file.write_all(&len.to_le_bytes())?;
        self.file.write_all(&checksum.to_le_bytes())?;

        // Write payload
        self.file.write_all(&serialized_payload)?;

        // Ensuring durability by fsync
        self.file.sync_all()?;

        Ok(())
    }
    
    
}

impl LogReader {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new().read(true).open(&path)?;

        Ok(Self { file })
    }

    pub fn read_entries(&mut self)-> io::Result<Vec<Operation>> {
        let mut entries = Vec::new();
        let mut buffer = Vec::new();

        self.file.read_to_end(&mut buffer)?;
        let mut cursor = 0;
        while cursor + 8 <= buffer.len() {
            let len = u32::from_le_bytes([buffer[cursor], buffer[cursor + 1], buffer[cursor + 2], buffer[cursor + 3]]) as usize;

            let expected_checksum = u32::from_le_bytes([buffer[cursor + 4], buffer[cursor + 5], buffer[cursor + 6], buffer[cursor + 7]]);

            cursor += 8;

            // checking payload availability
            if cursor + len > buffer.len() {
                break; // truncated entry
            }

            let payload = &buffer[cursor..cursor+len];
            let actual_checksum = crc32fast::hash(payload);

            if actual_checksum != expected_checksum {
                break;// corrupted entry
            }

            match bincode::decode_from_slice(payload, bincode::config::standard()) {
                Ok((op, _)) => entries.push(op),
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            }

            cursor += len;
        }

        Ok(entries)
    }
    
}