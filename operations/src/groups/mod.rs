pub mod add_member;
pub mod add_role;
pub mod backends;
pub mod create_group;
pub mod get_group;
pub mod join_request;
pub mod list_groups;
pub mod list_requests;
pub mod remove_member;
pub mod remove_role;
pub mod search_groups;
pub mod set_policies;
pub mod storage_routing;
pub mod update_group;

use aruna_core::errors::ConversionError;
use aruna_core::structs::GroupAuthorizationDocument;
use byteview::ByteView;

pub(crate) fn parse_auth_record(
    value: Option<ByteView>,
) -> Result<Option<GroupAuthorizationDocument>, ConversionError> {
    value
        .as_ref()
        .map(|value| GroupAuthorizationDocument::from_bytes(value.as_ref()))
        .transpose()
}
