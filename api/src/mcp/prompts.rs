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
        Some(
            "Create a valid Aruna dataset, optionally from the objects of one bucket, through \
             inventory, one round of questions, validation, and durable creation",
        ),
        Some(vec![
            PromptArgument::new("group_id")
                .with_description(
                    "Bare 26-character ULID of the group that will own the dataset, for example \
                     01JZ8Y6T0K4W7M2N9Q5R3S8V1X. Call the list_groups tool for the ids the caller \
                     may use. Optional when bucket is given: the dataset then goes to the group \
                     that owns the bucket, as list_buckets reports it.",
                )
                .with_required(false),
            PromptArgument::new("profile_id")
                .with_description(
                    "Optional bare 26-character ULID of a Profile document from the list_profiles \
                     tool. When given, the crate must conform to that Profile's SHACL rules.",
                )
                .with_required(false),
            PromptArgument::new("bucket")
                .with_description(
                    "Optional bucket name whose objects become the dataset's File entities, for \
                     example project-data. Call the list_buckets tool for the readable names.",
                )
                .with_required(false),
            PromptArgument::new("prefix")
                .with_description(
                    "Optional key prefix that narrows the bucket inventory, for example \
                     reads/2026/. Matched literally from the start of the key.",
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
    let argument = |name: &str| {
        arguments
            .get(name)
            .and_then(serde_json::Value::as_str)
            .filter(|value| !value.trim().is_empty())
            .map(str::to_string)
    };
    let owner = match (argument("group_id"), argument("bucket")) {
        (Some(group_id), _) => format!("group {group_id}"),
        (None, Some(bucket)) => {
            format!("the group that owns bucket {bucket}, as list_buckets reports it")
        }
        (None, None) => {
            return Err(ErrorData::invalid_params(
                "give group_id, a bare 26-character ULID such as 01JZ8Y6T0K4W7M2N9Q5R3S8V1X \
                 from the list_groups tool, or bucket, whose owning group is used",
                None,
            ));
        }
    };
    let profile = argument("profile_id")
        .map(|profile_id| {
            format!(" Use Profile document {profile_id} and read its SHACL Turtle rules.")
        })
        .unwrap_or_default();
    let scope = match (argument("bucket"), argument("prefix")) {
        (Some(bucket), Some(prefix)) => format!("bucket {bucket} under prefix {prefix}"),
        (Some(bucket), None) => format!("bucket {bucket}"),
        (None, Some(prefix)) => format!("the bucket the user names, under prefix {prefix}"),
        (None, None) => "the bucket the user names".to_string(),
    };
    let text = format!(
        "Create an Aruna dataset in {owner}.{profile} Read the aruna://docs/dataset-authoring resource and follow it. 1. Inventory {scope}: call list_buckets for the owning group, call list_objects with the prefix, and follow next_cursor until it is absent, then build one File entity per object with a contentUrl in s3://bucket/key form plus name, contentSize, encodingFormat, and dateModified taken from the listing. 2. Derive: read small text objects such as README, LICENSE, and CITATION.cff with read_object and propose a name, description, keywords, license, and creators, naming the source of each suggestion. 3. Ask once: in one compact message ask the user for what the data cannot answer, at least the name, description, creator or author, license IRI, datePublished, keywords, and which Profile from list_profiles, offering your suggestions as suggestions the user may edit. Never invent a person, an organization, an identifier, a license, or a date; a field the user declines stays absent and an optional field is never a reason to ask again. 4. Build one RO-Crate 1.3 JSON-LD graph whose root Dataset lists every File entity in hasPart and has exactly one conformsTo value. 5. Validate: call validate_dataset, repair every structural violation and Profile finding, and validate again until accepted. 6. Create: show the user the crate and call create_dataset with the chosen path and visibility only after they confirm. A successful create is durable acceptance, but HTTP 201 does not mean the dataset is immediately readable, so poll get_dataset until the raw revision is available."
    );
    Ok(
        GetPromptResult::new(vec![PromptMessage::new_text(Role::User, text)])
            .with_description("Inventory, ask, validate, and create an Aruna dataset")
            .into(),
    )
}
