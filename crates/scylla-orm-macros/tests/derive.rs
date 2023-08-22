use isolang::Language;
use scylla_orm_macros::CqlOrm;

#[derive(Debug, Default, Clone, CqlOrm, PartialEq, Eq)]
pub struct Document {
    pub id: xid::Id,
    pub status: i8,
    pub language: Language,
    pub authors: Vec<String>,
    pub content: Vec<u8>,

    pub _fields: Vec<String>, // fields with prefix `_` will be ignored by CqlOrm.
}

#[test]
fn derive_cql_orm_works() {
    assert_eq!(
        Document::fields(),
        vec!["id", "status", "language", "authors", "content"]
    );

    let doc = Document {
        id: xid::new(),
        status: 1,
        language: Language::Eng,
        authors: vec!["John".to_string(), "Doe".to_string()],
        content: vec![1, 2, 3],
        _fields: vec!["content".to_string()],
    };

    let cols = doc.to();
    assert_eq!(cols.len(), 5);
    assert_eq!(cols.get_as::<xid::Id>("id").unwrap(), doc.id);
    assert!(!cols.has("_fields"));

    let mut doc2: Document = Default::default();
    doc2.fill(&cols);
    assert_eq!(doc2._fields.len(), 0);
    doc2._fields = doc._fields.clone();
    assert_eq!(doc2, doc);
}
