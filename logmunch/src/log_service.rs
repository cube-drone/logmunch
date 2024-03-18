
struct LogService{

}

impl LogService {
    pub fn new() -> LogService {
        LogService{}
    }

    pub fn ingest(&self, data: crate::WritableEvent) -> &'static str {
        "OK"
    }
}