// Copyright 2020 The OctoSQL Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


// For now this just adds asynchronicity, not multiple partitions.

use std::sync::{Arc, mpsc};

use arrow::array::ArrayRef;
use arrow::record_batch::RecordBatch;
use anyhow::Result;

use crate::physical::expression::Expression;
use crate::physical::physical::*;
use crate::logical::logical::NodeMetadata;
use itertools::Either;

pub struct Shuffle {
    logical_metadata: NodeMetadata,
    source: Arc<dyn Node>,
}

impl Shuffle {
    pub fn new(logical_metadata: NodeMetadata, source: Arc<dyn Node>) -> Shuffle {
        Shuffle {
            logical_metadata,
            source,
        }
    }
}

impl Node for Shuffle {
    fn logical_metadata(&self) -> NodeMetadata {
        self.logical_metadata.clone()
    }

    fn run(
        &self,
        ctx: &ExecutionContext,
        produce: ProduceFn,
        meta_send: MetaSendFn,
    ) -> Result<()> {
        // TODO: Fix produce context pass?
        let (sender, receiver) = mpsc::sync_channel::<Either<RecordBatch, MetadataMessage>>(32);
        let child_ctx = ctx.clone();
        let source = self.source.clone();
        let handle = std::thread::spawn(move || {
            let _res = source.run(
                &child_ctx,
                &mut |_ctx, batch| {
                    sender.send(Either::Left(batch)).unwrap();
                    Ok(())
                },
                &mut |_ctx, meta| {
                    sender.send(Either::Right(meta));
                    Ok(())
                },
            ).unwrap();
        });

        for msg in receiver {
            match msg {
                Either::Left(batch) => produce(&ProduceContext{}, batch)?,
                Either::Right(meta) => meta_send(&ProduceContext{}, meta)?,
            };
        }

        handle.join().unwrap();

        Ok(())
    }
}
