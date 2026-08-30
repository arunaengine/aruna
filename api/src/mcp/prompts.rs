use super::McpServer;
use rmcp::model::{
    ErrorData, GetPromptRequestParams, GetPromptResponse, GetPromptResult, ListPromptsResult,
    Prompt, PromptArgument, PromptMessage, Role,
};
use rmcp::service::{RequestContext, RoleServer};

const PROMPT_NAME: &str = "create-dataset";

pub(crate) async fn list_prompts(
    _server: &McpServer,
    _context: RequestContext<RoleServer>,
) -> Result<ListPromptsResult, ErrorData> {
    Ok(ListPromptsResult::with_all_items(vec![Prompt::new(
        PROMPT_NAME,
        Some("Create a valid Aruna dataset through validation, repair, and durable creation"),
        Some(vec![
            PromptArgument::new("group_id")
                .with_description(
                    "Bare 26-character ULID of the group that will own the dataset, for example \
                     01JZ8Y6T0K4W7M2N9Q5R3S8V1X. Call the list_groups tool for the ids the caller \
                     may use.",
                )
                .with_required(true),
            PromptArgument::new("profile_id")
                .with_description(
                    "Optional bare 26-character ULID of a Profile document from the list_profiles \
                     tool. When given, the crate must conform to that Profile's SHACL rules.",
                )
                .with_required(false),
        ]),
    )]))
}

pub(crate) async fn get_prompt(
    _server: &McpServer,
    request: GetPromptRequestParams,
    _context: RequestContext<RoleServer>,
) -> Result<GetPromptResponse, ErrorData> {
    if request.name != PROMPT_NAME {
        return Err(ErrorData::invalid_params(
            format!("unknown prompt; this server offers only `{PROMPT_NAME}`"),
            None,
        ));
    }
    let arguments = request.arguments.unwrap_or_default();
    let group_id = arguments
        .get("group_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or_else(|| {
            ErrorData::invalid_params(
                "group_id is required and must be a bare 26-character ULID such as \
                 01JZ8Y6T0K4W7M2N9Q5R3S8V1X; call the list_groups tool for a valid id",
                None,
            )
        })?;
    let profile = arguments
        .get("profile_id")
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(|profile_id| {
            format!(" Use Profile document {profile_id} and read its SHACL Turtle rules.")
        })
        .unwrap_or_default();
    let text = format!(
        "Create an Aruna dataset in group {group_id}.{profile} Build one RO-Crate 1.3 JSON-LD graph, then call validate_dataset. Repair every structural violation and Profile finding and validate again until accepted. The root Dataset must have exactly one conformsTo value. File entities use contentUrl values in s3://bucket/key form. Only after validation succeeds call create_dataset with the chosen path and visibility. A successful create is durable acceptance, but HTTP 201 does not mean the dataset is immediately readable, so poll get_dataset until the raw revision is available."
    );
    Ok(
        GetPromptResult::new(vec![PromptMessage::new_text(Role::User, text)])
            .with_description("Validate, repair, and create an Aruna dataset")
            .into(),
    )
}
