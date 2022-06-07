use anyhow::{Context, Result};
use csv::{Reader, Writer};
use encoding_rs_io::DecodeReaderBytes;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::File;
use std::io::{Read, Write};
use std::path::Path;

pub struct CSV;

impl CSV {
    /// load from file, with UTF8 BOM detect
    pub fn load_csv_file<P: AsRef<Path>, D: DeserializeOwned>(csv: P) -> Result<Vec<D>> {
        let path = csv.as_ref();
        let file = File::open(path).with_context(|| path.display().to_string())?;
        let v = CSV::load_csv_read(DecodeReaderBytes::new(file))
            .with_context(|| path.display().to_string())?;
        Ok(v)
    }

    /// load from str, since str.as_bytes() implemented Read trait
    pub fn load_csv_read<R: Read, D: DeserializeOwned>(read: R) -> Result<Vec<D>> {
        let mut rdr = Reader::from_reader(read);
        let mut v = vec![];
        for result in rdr.deserialize::<D>() {
            let record: D = result?;
            v.push(record);
        }
        Ok(v)
    }

    pub fn save_csv_file<'a, P, S: 'a, I>(csv: P, iter: I) -> Result<()>
    where
        P: AsRef<Path>,
        S: Serialize,
        I: IntoIterator<Item = &'a S>,
    {
        let path = csv.as_ref();
        let file = File::create(path).with_context(|| path.display().to_string())?;
        let _ = CSV::save_csv_write(file, iter).with_context(|| path.display().to_string())?;
        Ok(())
    }

    pub fn save_csv_write<'a, W, S: 'a, I>(write: W, iter: I) -> Result<Writer<W>>
    where
        W: Write,
        S: Serialize,
        I: IntoIterator<Item = &'a S>,
    {
        // CSV header will be available since the structure of S is known.
        let mut wtr = Writer::from_writer(write);
        for record in iter.into_iter() {
            wtr.serialize(record)?;
        }
        wtr.flush()?;
        Ok(wtr)
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use chrono::NaiveDate;
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Deserialize, Serialize)]
    struct Bar {
        pub inst: String,
        pub date: NaiveDate,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: u64,
        // to test serde
        #[serde(skip_deserializing, default = "always_true")]
        pub test_skip: bool,
    }

    fn always_true() -> bool {
        true
    }

    #[test]
    fn save() -> Result<()> {
        let bar1 = Bar {
            inst: "IC2206".to_string(),
            date: NaiveDate::from_ymd(2022, 6, 6),
            open: 6048.6,
            high: 6186.4,
            low: 6031.2,
            close: 6157.8,
            volume: 90628,
            test_skip: true,
        };
        let bar2 = Bar {
            inst: "IF2206".to_string(),
            date: NaiveDate::from_ymd(2022, 6, 6),
            open: 4068.6,
            high: 4147.4,
            low: 4036.2,
            close: 4144.4,
            volume: 95173,
            test_skip: false,
        };
        {
            let v1 = vec![&bar1, &bar2];
            let wtr = CSV::save_csv_write(Vec::new(), &v1)?;
            let data = String::from_utf8(wtr.into_inner()?)?;
            println!("\nvec length {}, csv result is\n{}", v1.len(), data);
        }

        let v2 = vec![bar1, bar2];
        let wtr = CSV::save_csv_write(Vec::new(), &v2)?;
        let data = String::from_utf8(wtr.into_inner()?)?;
        println!("\nvec length {}, csv result is\n{}", v2.len(), data);
        Ok(())
    }

    #[test]
    fn load() -> Result<()> {
        // notice: there is no leading space in the line
        // 注意: 行首不能有空格
        let data = "
inst,date,open,high,low,close,volume,test_skip
IC2206,2022-06-06,6048.6,6186.4,6031.2,6157.8,90628,false
IF2206,2022-06-06,4068.6,4147.4,4036.2,4144.4,95173,false";

        let barlist: Vec<Bar> = CSV::load_csv_read(data.as_bytes())?;
        println!("bar: {:#?}", barlist);
        assert_eq!(barlist[0].test_skip, true);
        assert_eq!(barlist[1].test_skip, true);
        Ok(())
    }
}
