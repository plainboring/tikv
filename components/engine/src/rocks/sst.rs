use std::sync::Arc;

use super::util::get_fastest_supported_compression_type;
use super::{ColumnFamilyOptions, DBCompressionType, Env, EnvOptions, ExternalSstFileInfo, DB};
use crate::{CfName, CF_DEFAULT};
use engine_rocksdb::SstFileWriter;

pub struct SstWriterBuilder {
    cf: Option<CfName>,
    db: Option<Arc<DB>>,
    in_memory: bool,
}

impl SstWriterBuilder {
    pub fn new() -> SstWriterBuilder {
        SstWriterBuilder {
            cf: None,
            in_memory: false,
            db: None,
        }
    }

    pub fn set_db(mut self, db: Arc<DB>) -> Self {
        self.db = Some(db);
        self
    }

    pub fn set_in_memory(mut self, in_memory: bool) -> Self {
        self.in_memory = in_memory;
        self
    }

    pub fn set_cf(mut self, cf: CfName) -> Self {
        self.cf = Some(cf);
        self
    }

    pub fn build(self, path: &str) -> Result<SstWriter, String> {
        let mut env = None;
        let mut io_options = if let Some(db) = self.db.as_ref() {
            env = db.env();
            let handle = db
                .cf_handle(self.cf.unwrap_or(CF_DEFAULT))
                .map_or_else(|| Err(format!("CF {:?} is not found", self.cf)), Ok)?;
            db.get_options_cf(handle).clone()
        } else {
            ColumnFamilyOptions::new()
        };
        if self.in_memory {
            // Set memenv.
            let mem_env = Arc::new(Env::new_mem());
            io_options.set_env(mem_env.clone());
            env = Some(mem_env);
        } else if let Some(env) = env.as_ref() {
            io_options.set_env(env.clone());
        }
        io_options.compression(get_fastest_supported_compression_type());
        // in rocksdb 5.5.1, SstFileWriter will try to use bottommost_compression and
        // compression_per_level first, so to make sure our specified compression type
        // being used, we must set them empty or disabled.
        io_options.compression_per_level(&[]);
        io_options.bottommost_compression(DBCompressionType::Disable);
        let mut writer = SstFileWriter::new(EnvOptions::new(), io_options);
        writer.open(path)?;
        Ok(SstWriter { writer, env })
    }
}

pub struct SstWriter {
    writer: SstFileWriter,
    env: Option<Arc<Env>>,
}

impl SstWriter {
    /// Add key, value to currently opened file
    /// REQUIRES: key is after any previously added key according to comparator.
    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<(), String> {
        self.writer.put(key, val)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), String> {
        self.writer.delete(key)
    }

    pub fn delete_range(&mut self, begin_key: &[u8], end_key: &[u8]) -> Result<(), String> {
        self.writer.delete_range(begin_key, end_key)
    }

    pub fn file_size(&mut self) -> u64 {
        self.writer.file_size()
    }

    /// Finalize writing to sst file and close file.
    pub fn finish(mut self) -> Result<ExternalSstFileInfo, String> {
        self.writer.finish()
    }

    pub fn finish_into(mut self, buf: &mut Vec<u8>) -> Result<ExternalSstFileInfo, String> {
        use std::io::Read;
        if let Some(env) = self.env.take() {
            let sst_info = self.writer.finish()?;
            let p = sst_info.file_path();
            let path = p.as_os_str().to_str().ok_or_else(|| {
                format!(
                    "failed to sequential file bad path {}",
                    sst_info.file_path().display()
                )
            })?;
            let mut seq_file = env.new_sequential_file(path, EnvOptions::new())?;
            let len = seq_file
                .read_to_end(buf)
                .map_err(|e| format!("failed to read sequential file {:?}", e))?;
            if len as u64 != sst_info.file_size() {
                Err(format!(
                    "failed to read sequential file inconsistent length {} != {}",
                    len,
                    sst_info.file_size()
                ))
            } else {
                Ok(sst_info)
            }
        } else {
            Err(format!("failed to read sequential file no env provided"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rocks::util;
    use tempfile::Builder;

    #[test]
    fn test_somke() {
        let path = Builder::new().tempdir().unwrap();
        let engine = Arc::new(
            util::new_engine(path.path().to_str().unwrap(), None, &[CF_DEFAULT], None).unwrap(),
        );
        let (k, v) = (b"foo", b"bar");

        let p = path.path().join("sst");
        let mut writer = SstWriterBuilder::new()
            .set_cf(CF_DEFAULT)
            .set_db(engine.clone())
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let sst_file = writer.finish().unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        // There must be a file in disk.
        std::fs::metadata(p).unwrap();

        // Test in-memory sst writer.
        let p = path.path().join("inmem.sst");
        let mut writer = SstWriterBuilder::new()
            .set_in_memory(true)
            .set_cf(CF_DEFAULT)
            .set_db(engine.clone())
            .build(p.as_os_str().to_str().unwrap())
            .unwrap();
        writer.put(k, v).unwrap();
        let mut buf = vec![];
        let sst_file = writer.finish_into(&mut buf).unwrap();
        assert_eq!(sst_file.num_entries(), 1);
        assert!(sst_file.file_size() > 0);
        assert_eq!(buf.len() as u64, sst_file.file_size());
        // There must not be a file in disk.
        std::fs::metadata(p).unwrap_err();
    }
}
