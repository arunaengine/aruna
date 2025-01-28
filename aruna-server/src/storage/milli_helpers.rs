use crate::{
    constants::{field_names, Field, FIELDS},
    error::ArunaError,
    logerr,
};
use milli::heed::RwTxn;
use milli::{
    update::{IndexerConfig, Settings},
    FieldsIdsMap, Index,
};

pub(crate) fn prepopulate_fields<'a: 'b, 'b>(
    index: &'a Index,
    mut wtxn: &mut RwTxn<'b>,
) -> Result<(), ArunaError> {
    let mut field_ids_map = FieldsIdsMap::default();
    for (idx, Field { name, index }) in FIELDS.iter().enumerate() {
        let field_map_index = field_ids_map
            .insert(*name)
            .ok_or_else(|| ArunaError::ServerError(format!("Unable to pre-populate field")))?;
        assert_eq!(idx, *index as usize);
        assert_eq!(field_map_index, *index as u16);
    }

    // Make fields search and filterable
    let config = IndexerConfig::default();
    let mut settings = Settings::new(&mut wtxn, &index, &config);
    settings.set_searchable_fields(
        // Optimized searchable fields
        FIELDS
            .iter()
            .filter_map(|s| match s.name {
                field_names::NAME_FIELD
                | field_names::DESCRIPTION_FIELD
                | field_names::LABELS_FIELD
                | field_names::AUTHORS_FIELD
                | field_names::TAGS_FIELD
                | field_names::FIRST_NAME_FIELD
                | field_names::LAST_NAME_FIELD
                | field_names::LICENSE_TERMS => Some(s.name.to_string()),
                _ => None,
            })
            .collect(),
    );
    settings.set_filterable_fields(FIELDS.iter().map(|s| s.name.to_string()).collect());
    settings.execute(|_| (), || false).inspect_err(logerr!())?;

    index.put_fields_ids_map(&mut wtxn, &field_ids_map)?;
    // Ensure that the existing map has the expected field u32 mappings
    let existing_map = index.fields_ids_map(&wtxn)?;

    // This assertion is now more or less useless,
    // but for future idx shenanigans a good reference
    existing_map
        .iter()
        .zip(field_ids_map.iter())
        .for_each(|((got_id, got_name), (id, name))| {
            assert_eq!(got_id, id);
            assert_eq!(got_name, name);
        });

    Ok(())
}
