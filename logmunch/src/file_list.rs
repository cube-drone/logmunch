use std::fs;
use walkdir::WalkDir;
use std::collections::HashSet;
use anyhow::Result;

#[allow(unused_imports)] // (used in a test)
use std::time::{SystemTime, Duration};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileInfo{
    pub path: String,
    pub size_bytes: u64,
    pub last_modified: i64,
    pub day: i32,
    pub hour: i32,
    pub minute: i32,
    pub sort_key: i64,
    pub unique_id: String,
}

impl FileInfo{

    pub fn to_minute_id(&self) -> crate::minute_id::MinuteId{
        crate::minute_id::MinuteId::new(self.day as u32, self.hour as u32, self.minute as u32, &self.unique_id)
    }

    fn parse_path(path: &str) -> Result<(i32, i32, i32, String)>{
        let split = path.split("\\").collect::<Vec<&str>>();
        let day = split[1].parse::<i32>()?;
        let hour = split[2].parse::<i32>()?;
        let minute_and_unique_id = split[3].replace(".db", "");
        let split = minute_and_unique_id.split("-").collect::<Vec<&str>>();
        let minute = split[0].parse::<i32>()?;
        let unique_id = split[1].to_string();
        Ok((day, hour, minute, unique_id))
    }

    pub fn scan_and_clean(data_directory: &str, n_minutes: u64) -> Result<Vec<FileInfo>>{
        let mut files = Vec::new();
        let mut unopenable_files = HashSet::new();

        for entry in WalkDir::new(&data_directory){
            match entry{
                Ok(entry) => {
                    if entry.file_type().is_file() == false {
                        continue;
                    }
                    let path = entry.path().to_str();
                    match path{
                        Some(path) => {
                            let path = path.replace(data_directory, "");
                            if path.contains(".swp") || path.contains(".wal") {
                                // a file that is currently being written to by another process
                                // (do not open)
                                unopenable_files.insert(path.replace(".swp", "").replace(".wal", ""));
                            }
                            if unopenable_files.contains(path.replace(".db", "").as_str()){
                                continue;
                            }
                            match Self::parse_path(&path){
                                Ok((day, hour, minute, unique_id)) => {
                                    println!("{:?} {} {} {} {}", path, day, hour, minute, unique_id);
                                    let metadata = entry.metadata().unwrap();
                                    let size = metadata.len();
                                    let last_modified = metadata.modified().unwrap().elapsed().unwrap().as_secs();
                                    files.push(FileInfo{
                                        path: path.to_string(),
                                        size_bytes: size,
                                        last_modified: last_modified as i64,
                                        day,
                                        hour,
                                        minute,
                                        sort_key: day as i64 * 1000000 + hour as i64 * 10000 + minute as i64 * 100 + last_modified as i64,
                                        unique_id}
                                    );
                                },
                                Err(e) => {
                                    println!("Error: {}", e);
                                }
                            }
                        },
                        None => {
                            continue;
                        }
                    }
                },
                Err(e) => {
                    println!("Error: {}", e);
                }
            }
        }

        // sort the files by sort_key, with the most recent files first
        // and the oldest files last
        files.sort_by(|a, b| b.sort_key.cmp(&a.sort_key));

        // if there are more files than n_minutes, delete the oldest files
        if files.len() > n_minutes as usize {
            let extra_files = files.split_off(n_minutes as usize);
            for file in extra_files{
                let path = format!("{}{}", data_directory, file.path);
                Self::remove_file(path.as_str());
            }
        }

        // scan the data directory recursively and return a list of files as well as their sizes
        Ok(files)
    }

    ///
    /// Remove a file from the filesystem.
    ///
    fn remove_file(path: &str){
        // TODO: maybe we want to hook other behavior into here,
        //   like compressing the file and archiving it?
        match fs::remove_file(path){
            Ok(_) => {},
            Err(e) => {
                println!("Error: {}", e);
            }
        }
    }

}

#[allow(dead_code)]
fn prep_test_directory(data_directory: &str){
    let _ = fs::remove_dir_all(data_directory);
    fs::create_dir_all(data_directory).unwrap();

    let mut writer = crate::minute::ShardedMinute::new(1, data_directory.to_string() );
    let mut other_writer = crate::minute::Minute::new(1, 1, 1, &"borp", &data_directory ).unwrap();
    let mut other_other_writer = crate::minute::Minute::new(2, 3, 4, &"borp", &data_directory ).unwrap();

    let mut test_data_source = crate::minute::TestData::new();
    let mut test_data = Vec::new();
    for _ in 0..1000 {
        let data = crate::minute::generate_test_data(&mut test_data_source);
        test_data.push(data);
    }
    other_writer.write_second(test_data).unwrap();
    other_writer.seal().unwrap();

    let mut test_data_source = crate::minute::TestData::new();
    let mut test_data = Vec::new();
    for _ in 0..1000 {
        let data = crate::minute::generate_test_data(&mut test_data_source);
        test_data.push(data);
    }
    other_other_writer.write_second(test_data).unwrap();
    other_other_writer.seal().unwrap();

    let mut test_data_source = crate::minute::TestData::new();
    let mut test_data = Vec::new();
    for _ in 0..1000 {
        let data = crate::minute::generate_test_data(&mut test_data_source);
        test_data.push(data);
    }
    writer.write(test_data).unwrap();
    writer.seal().unwrap();
}

#[test]
fn test_directory_scan(){
    let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_millis() as u32;
    let test_directory = format!("./test_data/test_reader_{}", timestamp);

    prep_test_directory(&test_directory);

    let files = FileInfo::scan_and_clean(&test_directory, 5);

    for file in files.unwrap(){
        println!("{:?}", file);
    }
}