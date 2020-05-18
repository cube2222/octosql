use arrow::record_batch::RecordBatch;
use arrow::csv::reader;
use arrow::ipc::writer::*;
use std::fs::{File, read};
use arrow::util::pretty;
use std::path;
use std::result::*;
use std::io::Cursor;
use arrow::ipc::writer::FileWriter;
use arrow::csv;
use std::io;
use std::time;
use sled::{ConflictableTransactionError, IVec, open};
use arrow::util::pretty::pretty_format_batches;
use arrow::array::ArrayRef;
use arrow::datatypes::{Field, Schema};
use std::sync::Arc;

pub enum Error {
    IOError(io::Error),
    Unexpected,
}

pub trait Node {
    fn schema(&self) -> Result<Arc<Schema>, Error>;
    fn run(&self, produce: &dyn Fn(RecordBatch) -> Result<(), Error>) -> Result<(), Error>;
}

fn record_print(batch: RecordBatch) -> Result<(), Error> {
    println!("{}", batch.num_rows());
    //println!("{}", pretty_format_batches(&[batch]).unwrap());
    Ok(())
}

pub struct CSVSource<'a> {
    path: &'a str
}

impl<'a> CSVSource<'a> {
    fn new(path: &'a str) -> CSVSource<'a> {
        CSVSource { path }
    }
}

impl<'a> Node for CSVSource<'a> {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let file = File::open(self.path).unwrap();
        let r = csv::ReaderBuilder::new()
            .has_headers(true)
            .infer_schema(Some(10))
            .with_batch_size(8192)
            .build(file).unwrap();
        Ok(r.schema())
    }

    fn run(&self, produce: &dyn Fn(RecordBatch) -> Result<(), Error>) -> Result<(), Error> {
        let file = File::open(self.path).unwrap();
        let mut r = csv::ReaderBuilder::new()
            .has_headers(true)
            .infer_schema(Some(10))
            .with_batch_size(8192)
            .build(file).unwrap();
        loop {
            let maybe_rec = r.next().unwrap();
            match maybe_rec {
                None => break,
                Some(rec) => produce(rec),
            };
        }
        Ok(())
    }
}

pub struct Projection<'a, 'b> {
    fields: &'b [&'a str],
    source: Box<dyn Node>,
}

impl<'a, 'b> Projection<'a, 'b> {
    fn new(fields: &'b [&'a str], source: Box<dyn Node>) -> Projection<'a, 'b> {
        Projection { fields, source }
    }

    fn schema_from_source_schema(&self, source_schema: Arc<Schema>) -> Result<Arc<Schema>, Error> {
        let new_schema_fields: Vec<Field> = self.fields
            .into_iter()
            .map(|&field| source_schema.index_of(field).unwrap())
            .map(|i| source_schema.field(i).clone())
            .collect();
        Ok(Arc::new(Schema::new(new_schema_fields)))
    }
}

impl<'a, 'b> Node for Projection<'a, 'b> {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        let source_schema = self.source.schema()?;
        self.schema_from_source_schema(source_schema)
    }

    fn run(&self, produce: &dyn Fn(RecordBatch) -> Result<(), Error>) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let new_schema = self.schema_from_source_schema(source_schema.clone())?;

        let indices: Vec<usize> = self.fields.into_iter()
            .map(|&field| source_schema.index_of(field).unwrap())
            .collect();

        self.source.run(&|batch| {
            let new_columns: Vec<ArrayRef> = (&indices).into_iter()
                .map(|&i| batch.column(i).clone())
                .collect();

            let new_batch = RecordBatch::try_new(
                new_schema.clone(),
                new_columns,
            ).unwrap();

            produce(new_batch)?;
            Ok(())
        });
        Ok(())
    }
}

#[derive(Debug)]
enum QueueError {
    ArrowError(arrow::error::ArrowError),
    SledConflictableTransactionError(sled::ConflictableTransactionError),
    SerdeJSONError(serde_json::error::Error),
}

impl From<arrow::error::ArrowError> for QueueError {
    fn from(e: arrow::error::ArrowError) -> Self {
        QueueError::ArrowError(e)
    }
}

impl From<sled::ConflictableTransactionError> for QueueError {
    fn from(e: sled::ConflictableTransactionError) -> Self {
        QueueError::SledConflictableTransactionError(e)
    }
}

impl From<serde_json::error::Error> for QueueError {
    fn from(e: serde_json::error::Error) -> Self {
        QueueError::SerdeJSONError(e)
    }
}

impl From<QueueError> for sled::ConflictableTransactionError<QueueError> {
    fn from(e: QueueError) -> Self {
        sled::ConflictableTransactionError::<QueueError>::Abort(e)
    }
}

struct Queue<'a> {
    storage: &'a sled::TransactionalTree
}

fn decode_int_from_bytes(bytes: IVec) -> Result<i64, QueueError> {
    Ok(serde_json::from_slice::<i64>(bytes.as_ref())?)
}

impl Queue<'_> {
    fn new(storage: &sled::TransactionalTree) -> Queue {
        Queue { storage }
    }

    fn push(&self, batch: &RecordBatch) -> Result<(), QueueError> {
        let mut vec: Vec<u8> = vec![];
        let cursor = Cursor::new(&mut vec);
        let mut file_writer = FileWriter::try_new(cursor, batch.schema())?;
        file_writer.write(&batch)?;
        file_writer.finish()?;
        drop(file_writer);

        let end_bytes = self.storage.get("end").map_err(ConflictableTransactionError::from)?;
        let end = end_bytes.clone().map(decode_int_from_bytes).unwrap_or(Ok(0))?;
        let updated_end_bytes = serde_json::to_vec::<i64>(&(end + 1))?;
        self.storage.insert("end", updated_end_bytes).map_err(ConflictableTransactionError::from)?;

        let default_key = serde_json::to_vec::<i64>(&0)?;
        let element_key = end_bytes.unwrap_or(default_key.into());
        self.storage.insert(element_key, vec).map_err(ConflictableTransactionError::from)?;

        Ok(())
    }

    fn length(&self) -> Result<i64, QueueError> {
        let start_bytes = self.storage.get("start").map_err(ConflictableTransactionError::from)?;
        let start = start_bytes.map(decode_int_from_bytes).unwrap_or(Ok(0))?;
        let end_bytes = self.storage.get("end").map_err(ConflictableTransactionError::from)?;
        let end = end_bytes.map(decode_int_from_bytes).unwrap_or(Ok(0))?;

        Ok(end - start)
    }

    // fn pop(&mut self) -> Result<RecordBatch, QueueError> {
    //     let mut vec: Vec<u8> = vec![];
    //     let cursor = Cursor::new(&mut vec);
    //     let mut file_writer = FileWriter::try_new(cursor, batch.schema())?;
    //     file_writer.write(&batch)?;
    //     file_writer.finish()?;
    //     Ok(())
    // }
}

fn main() {
    let start_time = std::time::Instant::now();
    //
    // let file = File::open("cats.csv").unwrap();
    // let builder = reader::ReaderBuilder::default().with_batch_size(40000).has_headers(true).infer_schema(Some(16));
    // let mut cats_reader = builder.build(file).unwrap();
    //
    // let batch = cats_reader.next().unwrap().unwrap();

    let db = sled::open(path::Path::new("testdb")).unwrap();

    //let pipeline_data = db.open_tree("pipeline_data").unwrap();
    //let input_queue = db.open_tree("input_queue").unwrap();
    // let output_queue = db.open_tree("output_queue").unwrap();

    // // Prepare output queue.
    // output_queue.transaction::<_,_,i32>(|db| {
    //     let start = serde_json::to_vec::<i32>(&5).unwrap();
    //     let end = serde_json::to_vec::<i32>(&0).unwrap();
    //     db.insert("start", start)?;
    //     db.insert("end", end)?;
    //     Ok(())
    // }).unwrap();
    //
    // // Send batch to output queue.
    // let start = output_queue.transaction::<_,_,i32>(|db| {
    //     let start_bytes = db.get("start")?.unwrap();
    //     let start = serde_json::from_slice::<i32>(start_bytes.as_ref()).unwrap();
    //     Ok(start)
    // });

    // output_queue.transaction::<_,_,i32>(|db| {
    //     db.insert("batch", batch.into())?;
    //     Ok(())
    // }).unwrap();
    // let schema = batch.schema();
    // let mut vec: Vec<u8> = vec![];
    // let cursor = Cursor::new(&mut vec);
    // let mut file_writer = arrow::ipc::writer::FileWriter::try_new(cursor, schema).unwrap();
    // file_writer.write(&batch).unwrap();
    // file_writer.finish().unwrap();
    // drop(file_writer);
    //
    // println!("write to buffer {:?}", start_time.elapsed());
    //
    // let db = sled::open(path::Path::new("testdb")).unwrap();
    // println!("open db {:?}", start_time.elapsed());
    //
    // let output_queue = db.open_tree("output_queue").unwrap();
    // println!("open tree {:?}", start_time.elapsed());
    //
    // output_queue.transaction::<_, _, i32>(|db| {
    //     db.insert("data", vec.clone())?;
    //     Ok(())
    // }).unwrap();
    // println!("write data {:?}", start_time.elapsed());
    //
    // // Send batch to output queue.
    // let out_vec = output_queue.transaction::<_, _, i32>(|db| {
    //     Ok(db.get("data")?)
    // }).unwrap().unwrap();
    // println!("read data {:?}", start_time.elapsed());
    //
    // let cursor = Cursor::new(&out_vec);
    // let mut file_reader = arrow::ipc::reader::FileReader::try_new(cursor).unwrap();
    // let read_batch = file_reader.next().unwrap().unwrap();
    //
    // println!("read from buffer {:?}", start_time.elapsed());
    //
    // //pretty::print_batches(&vec!(read_batch));
    // println!("{:?}", read_batch.num_columns())


    // Queue Test
    // let db = sled::open(path::Path::new("testdb")).unwrap();
    // let tree = db.open_tree("input_queue").unwrap();
    // let length = tree.transaction::<_,_,QueueError>(|tree| {
    //     let input_queue = Queue::new(tree);
    //     input_queue.push(&batch)?;
    //     let len = input_queue.length()?;
    //     Ok(len)
    // });
    // println!("before flush {:?}", start_time.elapsed());
    // tree.flush();
    // println!("after flush {:?}", start_time.elapsed());
    // dbg!(length);

    // let db = sled::open(path::Path::new("testdb")).unwrap();
    // let tree = db.open_tree("input_queue").unwrap();
    // tree.set_merge_operator(|key, last_value, to_merge| {
    //     let cur_value = last_value.map_or(0, |last_value| {
    //         serde_json::from_slice::<i32>(last_value).unwrap()
    //     });
    //     let to_merge = serde_json::from_slice::<i32>(to_merge).unwrap();
    //     Some(serde_json::to_vec::<i32>(&(cur_value + to_merge)).unwrap())
    // });
    // tree.merge("data", serde_json::to_vec::<i32>(&5).unwrap()).unwrap();
    // tree.merge("data", serde_json::to_vec::<i32>(&4).unwrap()).unwrap();
    // tree.merge("data1", serde_json::to_vec::<i32>(&4).unwrap()).unwrap();
    // tree.merge("data", serde_json::to_vec::<i32>(&3).unwrap()).unwrap();
    // let (v1, v2) = tree.transaction::<_, _, i32>(|db| {
    //     Ok((serde_json::from_slice::<i32>(db.get("data")?.unwrap().as_ref()).unwrap(), serde_json::from_slice::<i32>(db.get("data1")?.unwrap().as_ref()).unwrap()))
    // }).unwrap();
    //
    // dbg!(v1);
    // dbg!(v2);
    //db.with_subscriber()

    let plan: Box<dyn Node> = Box::new(CSVSource::new("cats.csv"));
    let plan: Box<dyn Node> = Box::new(Projection::new(&["id", "name"], plan));
    let res = plan.run(&record_print);
    println!("{:?}", start_time.elapsed());
}
