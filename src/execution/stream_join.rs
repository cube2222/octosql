use crate::execution::datafusion::{create_key, create_row, GroupByScalar};
use crate::execution::execution::*;
use arrow::array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::{BTreeMap, HashMap};
use std::sync::{mpsc, Arc};

pub struct StreamJoin {
    source: Arc<dyn Node>,
    source_key_fields: Vec<String>,
    joined: Arc<dyn Node>,
    joined_key_fields: Vec<String>,
}

impl StreamJoin {
    pub fn new(
        source: Arc<dyn Node>,
        source_key_fields: Vec<String>,
        joined: Arc<dyn Node>,
        joined_key_fields: Vec<String>,
    ) -> StreamJoin {
        StreamJoin {
            source,
            source_key_fields,
            joined,
            joined_key_fields,
        }
    }
}

impl Node for StreamJoin {
    fn schema(&self) -> Result<Arc<Schema>, Error> {
        // Both without last row, and retraction added at end.
        let mut source_schema_fields = self.source.schema()?.fields().clone();
        source_schema_fields.truncate(source_schema_fields.len() - 1);
        let joined_schema_fields = self.joined.schema()?.fields().clone();
        let new_fields: Vec<Field> = source_schema_fields
            .iter()
            .map(|f| Field::new(f.name(), f.data_type().clone(), true))
            .chain(
                joined_schema_fields
                    .iter()
                    .map(|f| Field::new(f.name(), f.data_type().clone(), true)),
            )
            .collect();

        // TODO: Check if source and joined key types match.

        Ok(Arc::new(Schema::new(new_fields)))
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<(), Error> {
        let source_schema = self.source.schema()?;
        let source_key_indices: Vec<usize> = self
            .source_key_fields
            .iter()
            .map(|key_field| source_schema.index_of(key_field).unwrap())
            .collect();
        let joined_schema = self.joined.schema()?;
        let joined_key_indices: Vec<usize> = self
            .joined_key_fields
            .iter()
            .map(|key_field| joined_schema.index_of(key_field).unwrap())
            .collect();
        let output_schema = self.schema()?;

        // TODO: Fixme HashMap => BTreeMap
        let mut state_map: BTreeMap<
            Vec<GroupByScalar>,
            (
                HashMap<Vec<ScalarValue>, i64>,
                HashMap<Vec<ScalarValue>, i64>,
            ),
        > = BTreeMap::new();

        let key_types: Vec<DataType> = match self.source.schema() {
            Ok(schema) => self
                .source_key_fields
                .iter()
                .map(|field| schema.field_with_name(field).unwrap().data_type())
                .cloned()
                .collect(),
            _ => panic!("aaa"),
        };

        let (sender, receiver) = mpsc::sync_channel::<(usize, RecordBatch)>(32);

        let sender1 = sender.clone();
        let source = self.source.clone();
        let ctx1 = ctx.clone();
        let handle1 = std::thread::spawn(move || {
            let res = source.run(
                &ctx1,
                &mut |ctx, batch| {
                    sender1.send((0, batch));
                    Ok(())
                },
                &mut noop_meta_send,
            );
        });
        let sender2 = sender.clone();
        let joined = self.joined.clone();
        let ctx2 = ctx.clone();
        let handle2 = std::thread::spawn(move || {
            let res = joined.run(
                &ctx2,
                &mut |ctx, batch| {
                    sender2.send((1, batch));
                    Ok(())
                },
                &mut noop_meta_send,
            );
        });

        std::mem::drop(sender);

        let key_indices = vec![source_key_indices, joined_key_indices];

        for (source_index, batch) in receiver {
            let key_columns: Vec<ArrayRef> = key_indices[source_index]
                .iter()
                .map(|&i| batch.column(i))
                .cloned()
                .collect();

            let mut new_rows: BTreeMap<Vec<GroupByScalar>, Vec<Vec<ScalarValue>>> = BTreeMap::new();

            let mut required_capacity: usize = 0;

            for row in 0..batch.num_rows() {
                let mut key_vec = Vec::with_capacity(self.source_key_fields.len());
                for i in 0..key_columns.len() {
                    key_vec.push(GroupByScalar::Int64(0))
                }
                create_key(&key_columns, row, &mut key_vec);

                if let Some((source_rows, joined_rows)) = state_map.get(&key_vec) {
                    // The row will later be joined with the other source rows.
                    required_capacity += if source_index == 0 {
                        joined_rows.len()
                    } else {
                        source_rows.len()
                    };
                }

                let mut row_vec = Vec::with_capacity(batch.num_columns());
                for i in 0..batch.num_columns() {
                    row_vec.push(ScalarValue::Int64(0))
                }
                create_row(batch.columns(), row, &mut row_vec);

                let mut new_rows_for_key = new_rows.entry(key_vec).or_default();
                new_rows_for_key.push(row_vec);
            }

            let mut output_columns: Vec<ArrayRef> =
                Vec::with_capacity(source_schema.fields().len() + joined_schema.fields().len());

            if source_index == 0 {
                for column in 0..(source_schema.fields().len() - 1) {
                    // Omit the retraction field, it goes last.
                    match batch.column(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &new_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &new_row[column] {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                for column in 0..(joined_schema.fields().len() - 1) {
                    match joined_schema.field(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &other_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((_, state_other_rows)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &other_row[column]
                                                {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                let retraction_column = batch.num_columns() - 1;

                let mut array = BooleanBuilder::new(required_capacity);
                for (key, cur_new_rows) in &new_rows {
                    if let Some((_, state_other_rows)) = state_map.get(key) {
                        for (other_row, &count) in state_other_rows {
                            for new_row in cur_new_rows {
                                for repetition in 0..count {
                                    if let ScalarValue::Boolean(retraction) =
                                        new_row[retraction_column]
                                    {
                                        array.append_value(retraction);
                                    } else {
                                        panic!("invalid type");
                                    }
                                }
                            }
                        }
                    }
                }
                output_columns.push(Arc::new(array.finish()) as ArrayRef)
            } else {
                for column in 0..(source_schema.fields().len() - 1) {
                    match source_schema.field(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &other_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &other_row[column]
                                                {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                for column in 0..(joined_schema.fields().len() - 1) {
                    // Omit the retraction field, it goes last.
                    match batch.column(column).data_type() {
                        DataType::Int64 => {
                            let mut array = Int64Builder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Int64(n) = &new_row[column] {
                                                    array.append_value(*n);
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        DataType::Utf8 => {
                            let mut array = StringBuilder::new(required_capacity);
                            for (key, cur_new_rows) in &new_rows {
                                if let Some((state_other_rows, _)) = state_map.get(key) {
                                    for (other_row, &count) in state_other_rows {
                                        for new_row in cur_new_rows {
                                            for repetition in 0..count {
                                                if let ScalarValue::Utf8(text) = &new_row[column] {
                                                    array.append_value(text.as_str());
                                                } else {
                                                    panic!("invalid type");
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            output_columns.push(Arc::new(array.finish()) as ArrayRef)
                        }
                        _ => unimplemented!(),
                    }
                }

                let retraction_column = batch.num_columns() - 1;

                let mut array = BooleanBuilder::new(required_capacity);
                for (key, cur_new_rows) in &new_rows {
                    if let Some((state_other_rows, _)) = state_map.get(key) {
                        for (other_row, &count) in state_other_rows {
                            for new_row in cur_new_rows {
                                for repetition in 0..count {
                                    if let ScalarValue::Boolean(retraction) =
                                        new_row[retraction_column]
                                    {
                                        array.append_value(retraction);
                                    } else {
                                        panic!("invalid type");
                                    }
                                }
                            }
                        }
                    }
                }
                output_columns.push(Arc::new(array.finish()) as ArrayRef)
            }

            for (key, cur_new_rows) in new_rows {
                for mut row in cur_new_rows {
                    let (source_state, joined_state) = state_map
                        .entry(key.clone())
                        .or_insert((HashMap::default(), HashMap::default()));
                    let retraction = if let ScalarValue::Boolean(retraction) = row[row.len() - 1] {
                        retraction
                    } else {
                        panic!("invalid retraction type")
                    };
                    row.truncate(row.len() - 1);

                    let my_rows = if source_index == 0 {
                        source_state
                    } else {
                        joined_state
                    };
                    let my_entry = my_rows.entry(row).or_default();
                    if !retraction {
                        *my_entry += 1;
                    } else {
                        *my_entry -= 1;
                    }
                }
            }

            let output_batch = RecordBatch::try_new(output_schema.clone(), output_columns)?;
            if output_batch.num_rows() > 0 {
                produce(&ProduceContext {}, output_batch);
            }
        }

        handle1.join();
        handle2.join();

        Ok(())
    }
}
