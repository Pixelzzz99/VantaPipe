use std::sync::{Arc, Mutex};
use chrono::{DateTime, Utc};
use crate::error::EtlError;
use crate::extractor::Extractor;
use crate::transformer::Transformer;
use crate::loader::Loader;


#[derive(Debug)]
pub struct PipelineState {
    pub last_run: DateTime<Utc>,
    pub rows_processed: u64,
}

impl PipelineState {
    #[cfg(test)]
    pub fn new() -> Self {
        Self {
            last_run: Utc::now() - chrono::Duration::days(1),
            rows_processed: 0,
        }
    }
}

pub struct Pipeline {
    extractor: Box<dyn Extractor>,
    transformers: Vec<Box<dyn Transformer>>,
    loader: Box<dyn Loader>,
}

impl Pipeline {
    pub fn new(
    extractor: Box<dyn Extractor>,
    transformers: Vec<Box<dyn Transformer>>,
    loader: Box<dyn Loader>,
    ) -> Self {
        Self {extractor, transformers, loader}
    }

    pub async fn run(
        &self,
        state: &Arc<Mutex<PipelineState>>
    ) -> Result<u64, EtlError> {

        let last_run = {
            let s = state.lock().unwrap();
            s.last_run
        };

        let loaded_count = self.execute(last_run, None).await?;

        {
            let mut s = state.lock().unwrap();
            s.last_run = Utc::now();
            s.rows_processed += loaded_count;
        }

        Ok(loaded_count)
    }

    /// Standalone extract→transform→load, bounded by an explicit `until`
    /// instead of `PipelineState`. Used by the replay path (`src/replay.rs`)
    /// against a throwaway `Pipeline` built fresh from a config — never
    /// touches `PipelineState`, so it cannot corrupt a live cursor.
    pub async fn run_once_bounded(
        &self,
        last_run: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<u64, EtlError> {
        self.execute(last_run, until).await
    }

    async fn execute(
        &self,
        last_run: DateTime<Utc>,
        until: Option<DateTime<Utc>>,
    ) -> Result<u64, EtlError> {
        // EXTRACT
        log::info!("Extracting data since {}", last_run);
        let rows = self.extractor.extract(last_run, until).await?;
        let extracted_count = rows.len() as u64;
        log::info!("Extractred {} rows", extracted_count);

        if rows.is_empty(){
            return Ok(0);
        }


        //TRANSFORM
        let mut current_rows = rows;
        for transformer in &self.transformers {
            current_rows = transformer.transform(current_rows)?;
        }
        log::info!("After transforms: {} rows", current_rows.len());


        //LOAD
        let loaded_count = current_rows.len() as u64;
        self.loader.load(current_rows).await?;
        log::info!("Loaded {} rows ", loaded_count);

        Ok(loaded_count)
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use crate::types::{Row, Value, make_row};

    struct MockExtractor {
        rows: Vec<Row>,
    }

    #[async_trait]
    impl Extractor for MockExtractor {
        async fn extract(
            &self,
            _last_run: DateTime<Utc>,
            _until: Option<DateTime<Utc>>,
        ) -> Result<Vec<Row>, EtlError>{
            Ok(self.rows.clone())
        }
    }

    struct MockLoader {
        received: Arc<Mutex<Vec<Row>>>,
    }

    
    #[async_trait]
    impl Loader for MockLoader {
        async fn load(&self, rows: Vec<Row>) -> Result<(), EtlError> {
            let mut received = self.received.lock().unwrap();
            received.extend(rows);
            Ok(())
        }
    }

    struct DoubleTransformer;
    impl Transformer for DoubleTransformer {
        fn transform(&self, rows: Vec<Row>) -> Result<Vec<Row>, EtlError> {
            Ok(rows.into_iter().map(|mut row| {
                for val in row.values_mut() {
                    if let Value::Int(n) = val {
                        *n *= 2;
                    }
                }
                row
            }).collect())
        }
    }

    #[tokio::test]
    async fn test_pipeline_runs_etl_cycle(){
        let input_rows = vec![
            make_row(vec![("amount", Value::Int(10))]),
            make_row(vec![("amount", Value::Int(20))]),
        ];

        let received = Arc::new(Mutex::new(Vec::new()));

        let pipeline = Pipeline::new(
             Box::new(MockExtractor {rows: input_rows}),
             vec![Box::new(DoubleTransformer)],
             Box::new(MockLoader {received: received.clone()}),
        );

        let state = Arc::new(Mutex::new(PipelineState::new()));
        let count= pipeline.run(&state).await.unwrap();

        assert_eq!(count, 2);

        let got = received.lock().unwrap();
        assert_eq!(got[0].get("amount"), Some(&Value::Int(20)));
        assert_eq!(got[1].get("amount"), Some(&Value::Int(40)));

        let s = state.lock().unwrap();
        assert_eq!(s.rows_processed, 2);
    }


    #[tokio::test]
    async fn test_pipeline_empty_extract() {
        let received = Arc::new(Mutex::new(Vec::new()));
        let pipeline = Pipeline::new(
            Box::new(MockExtractor {rows: vec![] }),
            vec![],
            Box::new(MockLoader { received: received.clone() }),
        );

        let state = Arc::new(Mutex::new(PipelineState::new()));
        let count = pipeline.run(&state).await.unwrap();

        assert_eq!(count, 0);
        assert_eq!(received.lock().unwrap().len(), 0);

    }

}
