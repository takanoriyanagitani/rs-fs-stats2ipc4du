use std::collections::HashMap;
use std::io;
use std::sync::Arc;

use io::BufWriter;
use io::Write;

use io::BufRead;

use protobuf::CodedInputStream;
use protobuf::Message;

use protobuf::well_known_types::struct_::Struct;
use protobuf::well_known_types::struct_::Value;

use arrow_ipc::writer::StreamWriter;

use arrow_schema::DataType;
use arrow_schema::Field;
use arrow_schema::Schema;
use arrow_schema::SchemaRef;

use arrow_array::RecordBatch;

use arrow_array::builder::ArrayBuilder;

use arrow_array::builder::Int32Builder;
use arrow_array::builder::Int64Builder;

use arrow_array::builder::UInt32Builder;
use arrow_array::builder::UInt64Builder;

use arrow_array::builder::BooleanBuilder;

use arrow_array::builder::StringBuilder;

pub fn stat_schema() -> Schema {
    Schema::new(vec![
        Field::new("filename", DataType::Utf8, false),
        Field::new("dev", DataType::UInt64, true),
        Field::new("ino", DataType::UInt64, true),
        Field::new("mode", DataType::UInt32, true),
        Field::new("nlink", DataType::UInt64, true),
        Field::new("uid", DataType::UInt32, true),
        Field::new("gid", DataType::UInt32, true),
        Field::new("rdev", DataType::UInt64, true),
        Field::new("size", DataType::UInt64, true),
        Field::new("atime", DataType::Int64, true),
        Field::new("mtime", DataType::Int64, true),
        Field::new("ctime", DataType::Int64, true),
        Field::new("blksize", DataType::UInt64, true),
        Field::new("blocks", DataType::UInt64, true),
        Field::new("flags", DataType::UInt32, true),
        Field::new("gen", DataType::UInt32, true),
        Field::new("raw_os_error", DataType::Int32, true),
        Field::new("not_found", DataType::Boolean, true),
    ])
}

pub struct Builder {
    pub b_filename: StringBuilder,

    pub b_not_found: BooleanBuilder,

    pub b_dev: UInt64Builder,
    pub b_ino: UInt64Builder,
    pub b_mode: UInt32Builder,
    pub b_nlink: UInt64Builder,
    pub b_uid: UInt32Builder,
    pub b_gid: UInt32Builder,
    pub b_rdev: UInt64Builder,
    pub b_size: UInt64Builder,
    pub b_atime: Int64Builder,
    pub b_mtime: Int64Builder,
    pub b_ctime: Int64Builder,
    pub b_blksize: UInt64Builder,
    pub b_blocks: UInt64Builder,
    pub b_flags: UInt32Builder,
    pub b_gen: UInt32Builder,
    pub b_raw_os_error: Int32Builder,
}

impl Builder {
    pub fn len(&self) -> usize {
        self.b_filename.len()
    }

    pub fn is_empty(&self) -> bool {
        0 == self.len()
    }
}

pub fn d2ulong(d: f64) -> Option<u64> {
    let converted: u64 = d as u64;
    let cd: f64 = converted as f64;
    (cd == d).then_some(converted)
}

pub fn d2ilong(d: f64) -> Option<i64> {
    let converted: i64 = d as i64;
    let cd: f64 = converted as f64;
    (cd == d).then_some(converted)
}

pub fn d2uint(d: f64) -> Option<u32> {
    let converted: u32 = d as u32;
    let cd: f64 = converted as f64;
    (cd == d).then_some(converted)
}

pub fn val2ulong(v: &Value) -> Option<u64> {
    let f: f64 = v.number_value();
    d2ulong(f)
}

pub fn val2ilong(v: &Value) -> Option<i64> {
    let f: f64 = v.number_value();
    d2ilong(f)
}

pub fn val2uint(v: &Value) -> Option<u32> {
    let f: f64 = v.number_value();
    d2uint(f)
}

pub fn val2int(v: &Value) -> Option<i32> {
    let f: f64 = v.number_value();
    let converted: i32 = f as i32;
    let cd: f64 = converted as f64;
    (cd == f).then_some(converted)
}

pub fn map2ulong(m: &HashMap<String, Value>, key: &str) -> Option<u64> {
    let ov: Option<&Value> = m.get(key);
    let v: &Value = ov?;
    val2ulong(v)
}

pub fn map2uint(m: &HashMap<String, Value>, key: &str) -> Option<u32> {
    let ov: Option<&Value> = m.get(key);
    let v: &Value = ov?;
    val2uint(v)
}

pub fn map2ilong(m: &HashMap<String, Value>, key: &str) -> Option<i64> {
    let ov: Option<&Value> = m.get(key);
    let v: &Value = ov?;
    val2ilong(v)
}

pub fn map2int(m: &HashMap<String, Value>, key: &str) -> Option<i32> {
    let ov: Option<&Value> = m.get(key);
    let v: &Value = ov?;
    val2int(v)
}

pub fn map2ulong2builder(m: &HashMap<String, Value>, key: &str, bldr: &mut UInt64Builder) {
    let ou: Option<u64> = map2ulong(m, key);
    match ou {
        None => bldr.append_null(),
        Some(u) => bldr.append_value(u),
    }
}

pub fn map2uint2builder(m: &HashMap<String, Value>, key: &str, bldr: &mut UInt32Builder) {
    let ou: Option<u32> = map2uint(m, key);
    match ou {
        None => bldr.append_null(),
        Some(u) => bldr.append_value(u),
    }
}

pub fn map2ilong2builder(m: &HashMap<String, Value>, key: &str, bldr: &mut Int64Builder) {
    let ou: Option<i64> = map2ilong(m, key);
    match ou {
        None => bldr.append_null(),
        Some(i) => bldr.append_value(i),
    }
}

pub fn map2int2builder(m: &HashMap<String, Value>, key: &str, bldr: &mut Int32Builder) {
    let ou: Option<i32> = map2int(m, key);
    match ou {
        None => bldr.append_null(),
        Some(i) => bldr.append_value(i),
    }
}

pub trait BatSink {
    fn consume(&mut self, bat: &RecordBatch) -> Result<(), io::Error>;
    fn close(self) -> Result<(), io::Error>;
}

impl Builder {
    pub fn to_bat_sink<S>(&mut self, sch: SchemaRef, sink: &mut S) -> Result<(), io::Error>
    where
        S: BatSink,
    {
        let bat: RecordBatch = RecordBatch::try_new(
            sch,
            vec![
                Arc::new(self.b_filename.finish()),
                Arc::new(self.b_dev.finish()),
                Arc::new(self.b_ino.finish()),
                Arc::new(self.b_mode.finish()),
                Arc::new(self.b_nlink.finish()),
                Arc::new(self.b_uid.finish()),
                Arc::new(self.b_gid.finish()),
                Arc::new(self.b_rdev.finish()),
                Arc::new(self.b_size.finish()),
                Arc::new(self.b_atime.finish()),
                Arc::new(self.b_mtime.finish()),
                Arc::new(self.b_ctime.finish()),
                Arc::new(self.b_blksize.finish()),
                Arc::new(self.b_blocks.finish()),
                Arc::new(self.b_flags.finish()),
                Arc::new(self.b_gen.finish()),
                Arc::new(self.b_raw_os_error.finish()),
                Arc::new(self.b_not_found.finish()),
            ],
        )
        .map_err(io::Error::other)?;
        sink.consume(&bat)
    }
}

impl Builder {
    pub fn append_from_proto(&mut self, pmap: &Struct) -> Result<(), io::Error> {
        let hmap: &HashMap<String, Value> = &pmap.fields;

        let ovfilename: Option<&Value> = hmap.get("filename");
        let Some(vfilename) = ovfilename else {
            return Err(io::Error::other("filename missing in stat proto"));
        };
        let filename = vfilename.string_value();
        self.b_filename.append_value(filename);

        let ovnot_found: Option<&Value> = hmap.get("not_found");
        match ovnot_found {
            Some(v) => {
                let b = v.bool_value();
                self.b_not_found.append_value(b);
            }
            None => {
                self.b_not_found.append_null();
            }
        }

        // UInt64 fields
        map2ulong2builder(hmap, "dev", &mut self.b_dev);
        map2ulong2builder(hmap, "ino", &mut self.b_ino);
        map2ulong2builder(hmap, "nlink", &mut self.b_nlink);
        map2ulong2builder(hmap, "size", &mut self.b_size);
        map2ulong2builder(hmap, "blksize", &mut self.b_blksize);
        map2ulong2builder(hmap, "blocks", &mut self.b_blocks);
        map2ulong2builder(hmap, "rdev", &mut self.b_rdev);

        // UInt32 fields
        map2uint2builder(hmap, "mode", &mut self.b_mode);
        map2uint2builder(hmap, "uid", &mut self.b_uid);
        map2uint2builder(hmap, "gid", &mut self.b_gid);
        map2uint2builder(hmap, "flags", &mut self.b_flags);
        map2uint2builder(hmap, "gen", &mut self.b_gen);

        // Int64 fields
        map2ilong2builder(hmap, "atime", &mut self.b_atime);
        map2ilong2builder(hmap, "mtime", &mut self.b_mtime);
        map2ilong2builder(hmap, "ctime", &mut self.b_ctime);

        // Int32: raw_os_error
        map2int2builder(hmap, "raw_os_error", &mut self.b_raw_os_error);

        Ok(())
    }
}

impl Builder {
    pub fn from_capacity_ext(cap: usize, sz_per_item4str: usize) -> Self {
        Self {
            b_filename: StringBuilder::with_capacity(cap, cap * sz_per_item4str),
            b_not_found: BooleanBuilder::with_capacity(cap),

            b_dev: UInt64Builder::with_capacity(cap),
            b_ino: UInt64Builder::with_capacity(cap),
            b_mode: UInt32Builder::with_capacity(cap),
            b_nlink: UInt64Builder::with_capacity(cap),
            b_uid: UInt32Builder::with_capacity(cap),
            b_gid: UInt32Builder::with_capacity(cap),
            b_rdev: UInt64Builder::with_capacity(cap),
            b_size: UInt64Builder::with_capacity(cap),
            b_atime: Int64Builder::with_capacity(cap),
            b_mtime: Int64Builder::with_capacity(cap),
            b_ctime: Int64Builder::with_capacity(cap),
            b_blksize: UInt64Builder::with_capacity(cap),
            b_blocks: UInt64Builder::with_capacity(cap),
            b_flags: UInt32Builder::with_capacity(cap),
            b_gen: UInt32Builder::with_capacity(cap),
            b_raw_os_error: Int32Builder::with_capacity(cap),
        }
    }

    pub fn from_capacity(cap: usize) -> Self {
        Self::from_capacity_ext(cap, 256)
    }
}

impl Builder {
    pub fn structs2sink<R, S>(
        capacity: usize,
        sz_per_item4str: usize,
        sch: SchemaRef,
        mut structs: R,
        mut sink: S,
    ) -> Result<(), io::Error>
    where
        R: BufRead,
        S: BatSink,
    {
        let mut me: Self = Self::from_capacity_ext(capacity, sz_per_item4str);

        let mut cis = CodedInputStream::from_buf_read(&mut structs);
        let mut buf: Struct = Struct::default();

        while !cis.eof().map_err(io::Error::other)? {
            if capacity == me.len() {
                me.to_bat_sink(sch.clone(), &mut sink)?;
            }

            buf.clear();
            cis.merge_message(&mut buf).map_err(io::Error::other)?;

            me.append_from_proto(&buf)?;
        }

        if !me.is_empty() {
            me.to_bat_sink(sch.clone(), &mut sink)?;
        }

        sink.close()?;

        Ok(())
    }
}

pub struct IpcWriter<W>(pub StreamWriter<W>);

impl<W> BatSink for IpcWriter<W>
where
    W: Write,
{
    fn consume(&mut self, bat: &RecordBatch) -> Result<(), io::Error> {
        self.0.write(bat).map_err(io::Error::other)
    }

    fn close(mut self) -> Result<(), io::Error> {
        self.0.flush().map_err(io::Error::other)?;
        self.0.finish().map_err(io::Error::other)?;
        Ok(())
    }
}

pub fn wtr2sink<W>(wtr: W, sch: &Schema) -> Result<impl BatSink, io::Error>
where
    W: Write,
{
    let sw: StreamWriter<W> = StreamWriter::try_new(wtr, sch).map_err(io::Error::other)?;
    Ok(IpcWriter(sw))
}

impl Builder {
    pub fn reader2maps2bat2wtr<R, W>(
        capacity: usize,
        sz_per_item4str: usize,
        sref: SchemaRef,
        rdr: R,
        wtr: W,
    ) -> Result<(), io::Error>
    where
        R: BufRead,
        W: Write,
    {
        let s2: SchemaRef = sref.clone();
        let sink = wtr2sink(wtr, &s2)?;
        Self::structs2sink(capacity, sz_per_item4str, sref, rdr, sink)
    }
}

impl Builder {
    pub fn stdin2maps2bat2stdout(capacity: usize, sz_per_item4str: usize) -> Result<(), io::Error> {
        let sch: Schema = stat_schema();

        let o = io::stdout();
        let mut ol = o.lock();
        Self::reader2maps2bat2wtr(
            capacity,
            sz_per_item4str,
            sch.into(),
            io::stdin().lock(),
            BufWriter::new(&mut ol),
        )?;
        ol.flush()
    }
}

pub const CAP_DEFAULT: usize = 8192;
pub const STR_SZ_DEFAULT: usize = 256;

#[derive(Debug, Clone, Copy)]
pub struct Config {
    pub capacity: usize,
    pub string_size_per_item: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            capacity: CAP_DEFAULT,
            string_size_per_item: STR_SZ_DEFAULT,
        }
    }
}
