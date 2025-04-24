use super::search::Search;
use crate::{error::ArunaError, models::models::Resource, persistence::persistence::Authorize};
use roaring::RoaringBitmap;
use std::fs;
use tantivy::{
    Document, Index, IndexReader, IndexWriter, TantivyDocument,
    collector::{FilterCollector, TopDocs},
    directory::MmapDirectory,
    query::QueryParser,
    schema::{FAST, Field, INDEXED, OwnedValue, STORED, Schema, TEXT},
};
use ulid::Ulid;

pub struct TantivySearch {
    index: Index,
    //writer: std::sync::Mutex<IndexWriter>,
    writer: tokio::sync::mpsc::Sender<(u32, Resource)>,
    reader: IndexReader,
    schema: Schema,
    fields: Fields,
}

impl std::fmt::Debug for TantivySearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TantivySearch")
            .field("index", &self.index)
            .field("writer", &self.writer)
            .field("schema", &self.schema)
            .field("fields", &self.fields)
            .finish()
    }
}

#[derive(Clone, Debug)]
struct Fields {
    ids: Field,
    idx: Field,
    name: Field,
    description: Field,
    variant: Field,
    labels: Field,
    content_len: Field,
    count: Field,
    visibility: Field,
    created_at: Field,
    last_modified: Field,
    authors: Field,
    license: Field,
    locked: Field,
    deleted: Field,
}

pub struct TantivyConfig {
    pub path: String,
    pub index_buffer: usize,
    pub resources: tokio::sync::mpsc::Receiver<(u32, Resource)>, // TODO: replace with channel receiver
                                                                 //pub _users: tokio::sync::mpsc::Receiver<User>, // TODO: replace with channel receiver
}

impl Search for TantivySearch {
    type SearchConfig = TantivyConfig;
    #[tracing::instrument(level = "trace", skip(config))]
    fn new(mut config: Self::SearchConfig) -> Result<Self, ArunaError> {
        let (update_queue_sdx, mut update_queue_rcv) = tokio::sync::mpsc::channel(1000);
        // First we need to define a schema ...

        // `TEXT` means the field should be tokenized and indexed,
        // along with its term frequency and term positions.
        //
        // `STORED` means that the field will also be saved
        // in a compressed, row-oriented key-value store.
        // This store is useful to reconstruct the
        // documents that were selected during the search phase.
        let mut schema_builder = Schema::builder();
        let ids = schema_builder.add_bytes_field("id", FAST | STORED | INDEXED);
        let idx = schema_builder.add_u64_field("idx", FAST | STORED | INDEXED);
        let name = schema_builder.add_text_field("name", TEXT);
        let description = schema_builder.add_text_field("description", TEXT);
        let variant = schema_builder.add_u64_field("variant", STORED);
        let labels = schema_builder.add_json_field("labels", TEXT);
        let content_len = schema_builder.add_u64_field("content_len", STORED);
        let count = schema_builder.add_u64_field("count", STORED);
        let visibility = schema_builder.add_u64_field("visibility", STORED);
        let created_at = schema_builder.add_date_field("created_at", STORED);
        let last_modified = schema_builder.add_date_field("last_modified", STORED);
        let authors = schema_builder.add_json_field("authors", TEXT);
        let license = schema_builder.add_bytes_field("license", FAST | STORED);
        let locked = schema_builder.add_bool_field("locked", FAST);
        let deleted = schema_builder.add_bool_field("deleted", FAST);
        let fields = Fields {
            ids,
            idx,
            name,
            description,
            variant,
            labels,
            content_len,
            count,
            visibility,
            created_at,
            last_modified,
            authors,
            license,
            locked,
            deleted,
        };

        fs::create_dir_all(&config.path)?;
        let dir = MmapDirectory::open(config.path)
            .map_err(|e| ArunaError::DatabaseError(e.to_string()))?;

        let schema = schema_builder.build();

        // Indexing documents
        let index = Index::open_or_create(dir, schema.clone())?;

        // idx docs send from store init
        let mut index_writer: IndexWriter = index.writer(config.index_buffer)?;
        while let Some((idx, resource)) = config.resources.blocking_recv() {
            let doc = fields.create_doc(idx, resource);
            // Let's index one documents!
            index_writer.add_document(doc)?;
        }
        index_writer.commit()?;

        // Idx task for batching
        let fields_clone = fields.clone();
        std::thread::spawn(move || {
            let mut buffer: Vec<(u32, Resource)> = Vec::new();
            loop {
                update_queue_rcv.blocking_recv_many(&mut buffer, 999);
                //println!("Queued {}", buffer.len());
                while let Some((idx, resource)) = buffer.pop() {
                    let doc = fields_clone.create_doc(idx, resource);
                    // Let's index one documents!
                    if let Err(err) = index_writer.add_document(doc) {
                        println!("{err}");
                    };
                }
                if let Err(err) = index_writer.commit() {
                    println!("{err}");
                };
            }
        });

        // We need to call .commit() explicitly to force the
        // index_writer to finish processing the documents in the queue,
        // flush the current index to the disk, and advertise
        // the existence of new documents.

        let reader = index.reader()?;

        Ok(TantivySearch {
            index,
            //writer: Mutex::new(index_writer),
            writer: update_queue_sdx,
            reader,
            fields,
            schema,
        })
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn search<A: Authorize>(
        &self,
        universe: RoaringBitmap,
        query: String,
    ) -> Result<Vec<String>, ArunaError> {
        let searcher = self.reader.searcher();
        let parser = QueryParser::for_index(
            &self.index,
            vec![
                self.fields.ids,
                self.fields.idx,
                self.fields.name,
                self.fields.description,
                self.fields.variant,
                self.fields.labels,
                self.fields.content_len,
                self.fields.count,
                self.fields.visibility,
                self.fields.created_at,
                self.fields.last_modified,
                self.fields.authors,
                self.fields.license,
                self.fields.locked,
                self.fields.deleted,
            ],
        );
        let parsed_query = parser.parse_query(&query).unwrap();
        let universe = universe.clone();
        let idx_collector = FilterCollector::new(
            "idx".to_string(),
            move |idx: u64| universe.contains(idx as u32),
            TopDocs::with_limit(100),
        );
        let result = searcher.search(&parsed_query, &idx_collector).unwrap();
        let mut docs = Vec::new();
        for (_, addr) in result {
            docs.push(
                searcher
                    .doc::<TantivyDocument>(addr)
                    .unwrap()
                    .to_json(&self.schema),
            );
        }

        Ok(docs)
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn add_resource(&self, idx: u32, resource: Resource) -> Result<(), ArunaError> {
        //        let mut writer = self.writer.lock().expect("Mutex panicked");
        //        let doc = self.fields.create_doc(idx, resource);
        //        writer.add_document(doc)?;
        //        writer.commit()?;

        // drop(writer);

        self.writer
            .send((idx, resource))
            .await
            .map_err(|e| ArunaError::ServerError(e.to_string()))?;

        Ok(())
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn remove(&self, _id: Ulid) -> Result<(), ArunaError> {
        todo!()
    }

    #[tracing::instrument(level = "trace", skip(self))]
    async fn purge(&self) -> Result<(), ArunaError> {
        // let _ = self
        //     .writer
        //     .lock()
        //     .expect("Mutex panicked")
        //     .delete_all_documents()?;
        Ok(())
    }
}

impl Fields {
    fn create_doc(&self, idx: u32, resource: Resource) -> TantivyDocument {
        let mut doc = TantivyDocument::default();
        doc.add_bytes(self.ids, resource.id.to_bytes().as_slice());
        doc.add_u64(self.idx, idx as u64);
        doc.add_text(self.name, resource.name);
        doc.add_text(self.description, resource.description);
        doc.add_u64(self.variant, resource.variant as u64);
        doc.add_object(
            self.labels,
            resource
                .labels
                .into_iter()
                .map(|kv| (kv.key, OwnedValue::Str(kv.value)))
                .collect(),
        );
        doc.add_u64(self.content_len, resource.content_len);
        doc.add_u64(self.count, resource.count);
        doc.add_u64(self.visibility, resource.visibility as u64);
        doc.add_date(
            self.created_at,
            tantivy::DateTime::from_timestamp_secs(resource.created_at.timestamp()),
        );
        doc.add_date(
            self.last_modified,
            tantivy::DateTime::from_timestamp_secs(resource.last_modified.timestamp()),
        );
        doc.add_object(
            self.authors,
            resource
                .authors
                .into_iter()
                .map(|author| {
                    (
                        "author".to_string(),
                        OwnedValue::Object(vec![
                            ("first".to_string(), OwnedValue::Str(author.first)),
                            ("last".to_string(), OwnedValue::Str(author.last)),
                            ("id".to_string(), OwnedValue::Str(author.id)),
                        ]),
                    )
                })
                .collect(),
        );
        doc.add_bytes(self.license, resource.license_id.to_bytes().as_slice());
        doc.add_bool(self.locked, resource.locked);
        doc.add_bool(self.deleted, resource.deleted);
        doc
    }
}
