use anyhow::Result;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct MinuteId{
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub unique_id: String,
}

impl PartialOrd for MinuteId {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.day < other.day {
            return Some(std::cmp::Ordering::Less);
        }
        if self.day > other.day {
            return Some(std::cmp::Ordering::Greater);
        }
        if self.hour < other.hour {
            return Some(std::cmp::Ordering::Less);
        }
        if self.hour > other.hour {
            return Some(std::cmp::Ordering::Greater);
        }
        if self.minute < other.minute {
            return Some(std::cmp::Ordering::Less);
        }
        if self.minute > other.minute {
            return Some(std::cmp::Ordering::Greater);
        }
        Some(self.unique_id.cmp(&other.unique_id))
    }
}

impl Ord for MinuteId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl MinuteId{
    pub fn new(day: u32, hour: u32, minute: u32, unique_id: &str) -> MinuteId {
        MinuteId{
            day,
            hour,
            minute,
            unique_id: unique_id.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}-{}-{}-{}", self.day, self.hour, self.minute, self.unique_id)
    }

    pub fn from_string(s: &str) -> Result<MinuteId> {
        let split = s.split("-").collect::<Vec<&str>>();
        let day = split[0].parse::<u32>()?;
        let hour = split[1].parse::<u32>()?;
        let minute = split[2].parse::<u32>()?;
        let unique_id = split[3].to_string();
        Ok(MinuteId{
            day,
            hour,
            minute,
            unique_id,
        })
    }
}
