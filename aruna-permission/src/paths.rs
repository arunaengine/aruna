use blake3::Hash as Blake3Hash;
use nom::{
    Err as NomErr, IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while1},
    combinator::map,
    error::{Error as NomError, ErrorKind},
};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use ulid::Ulid;

use crate::error::PathError;

/// Result type for path operations.
pub type Result<T> = std::result::Result<T, PathError>;

/// Represents the components that can make up a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathComponent {
    /// Realm ID - this is always the first component
    RealmId(Ulid),
    /// Admin section: `/a`
    Admin,
    /// Groups management section: `/g` within admin
    Groups,
    /// Policies management section: `/p` within admin
    Policies,
    /// Specific policy ID: an ULID after `/p/`
    PolicyId(Ulid),
    /// Group section: `/g`
    Group,
    /// Group ID: an ULID after `/g/`
    GroupId(Ulid),
    /// Admin section within a group: `/a`
    GroupAdmin,
    /// Resources section: `/r`
    Resources,
    /// Data resources section: `/d`
    Data,
    /// Bucket name for data resources
    Bucket(String),
    /// Key name for data resources (can be a key prefix for wildcards)
    Key(String),
    /// Content hash for data resources
    ContentHash(Blake3Hash),
    /// Metadata resources section: `/m`
    Metadata,
    /// Project ID for metadata resources
    ProjectId(Ulid),
    /// Object ID in a metadata path
    ObjectId(Ulid),
    /// Wildcard: `/*` - can be placed at the end of any valid path
    Wildcard,
}

/// Represents a path in the custom path system.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path {
    components: Vec<PathComponent>,
}

impl Path {
    /// Returns a new path builder.
    pub fn builder() -> PathBuilder {
        PathBuilder::new()
    }

    /// Returns the realm ID of the path.
    pub fn realm_id(&self) -> Option<Ulid> {
        if let Some(PathComponent::RealmId(ulid)) = self.components.first() {
            Some(*ulid)
        } else {
            None
        }
    }

    /// Returns true if the path contains wildcards.
    pub fn has_wildcards(&self) -> bool {
        self.components
            .iter()
            .any(|comp| matches!(comp, PathComponent::Wildcard))
    }

    /// Returns the components that make up this path.
    pub fn components(&self) -> &[PathComponent] {
        &self.components
    }

    /// Parses a string into a Path.
    pub fn parse(input: &str) -> Result<Self> {
        match path_parser.parse(input) {
            Ok((remaining, components)) => {
                if remaining.is_empty() {
                    let path = Path { components };
                    path.validate()?;
                    Ok(path)
                } else {
                    Err(PathError::ParseError(format!(
                        "Unparsed input remaining: '{}'",
                        remaining
                    )))
                }
            }
            Err(e) => Err(PathError::ParseError(format!(
                "Failed to parse path: {}",
                e
            ))),
        }
    }

    /// Validates that the path is well-formed.
    pub fn validate(&self) -> Result<()> {
        if self.components.is_empty() {
            return Err(PathError::ValidationError(
                "Path cannot be empty".to_string(),
            ));
        }

        // Ensure first component is a realm ID
        if !matches!(self.components.first(), Some(PathComponent::RealmId(_))) {
            return Err(PathError::ValidationError(
                "Path must start with a realm ID".to_string(),
            ));
        }

        // Check component sequence validity
        let mut iter = self.components.iter().peekable();

        // Skip the first component (RealmId) - already validated
        if let Some(_) = iter.next() {
            while let Some(current) = iter.next() {
                let next = iter.peek();

                // Validate component transitions
                match current {
                    PathComponent::RealmId(_) => {
                        // RealmId can only be followed by Admin or Group
                        if !matches!(
                            next,
                            Some(PathComponent::Admin) | Some(PathComponent::Group) | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Realm ID can only be followed by /a or /g".to_string(),
                            ));
                        }
                    }
                    PathComponent::Admin => {
                        // Admin can be followed by Groups, Policies, or Wildcard, or nothing
                        if !matches!(
                            next,
                            Some(PathComponent::Groups)
                                | Some(PathComponent::Policies)
                                | Some(PathComponent::Wildcard)
                                | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Admin can only be followed by /g, /p, or /*".to_string(),
                            ));
                        }
                    }
                    PathComponent::Groups => {
                        // Admin Groups can only be followed by Wildcard or nothing
                        if !matches!(next, Some(PathComponent::Wildcard) | None) {
                            return Err(PathError::ValidationError(
                                "Admin Groups can only be followed by /* or nothing".to_string(),
                            ));
                        }
                    }
                    PathComponent::Policies => {
                        // Admin Policies can be followed by PolicyId, Wildcard, or nothing
                        if !matches!(
                            next,
                            Some(PathComponent::PolicyId(_)) | Some(PathComponent::Wildcard) | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Admin Policies can only be followed by a policy ID, /*, or nothing".to_string(),
                            ));
                        }
                    }
                    PathComponent::PolicyId(_) => {
                        // PolicyId can only be followed by Wildcard or nothing
                        if !matches!(next, Some(PathComponent::Wildcard) | None) {
                            return Err(PathError::ValidationError(
                                "Policy ID can only be followed by /* or nothing".to_string(),
                            ));
                        }
                    }
                    PathComponent::Group => {
                        // Group must be followed by GroupId
                        if !matches!(next, Some(PathComponent::GroupId(_))) {
                            return Err(PathError::ValidationError(
                                "Group must be followed by a group ID".to_string(),
                            ));
                        }
                    }
                    PathComponent::GroupId(_) => {
                        // GroupId can be followed by GroupAdmin, Resources, Wildcard, or nothing
                        if !matches!(
                            next,
                            Some(PathComponent::GroupAdmin)
                                | Some(PathComponent::Resources)
                                | Some(PathComponent::Wildcard)
                                | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Group ID can only be followed by /a, /r, /*, or nothing"
                                    .to_string(),
                            ));
                        }
                    }
                    PathComponent::GroupAdmin => {
                        // GroupAdmin can only be followed by Wildcard or nothing
                        if !matches!(next, Some(PathComponent::Wildcard) | None) {
                            return Err(PathError::ValidationError(
                                "Group Admin can only be followed by /* or nothing".to_string(),
                            ));
                        }
                    }
                    PathComponent::Resources => {
                        // Resources must be followed by Data or Metadata or Wildcard
                        if !matches!(
                            next,
                            Some(PathComponent::Data)
                                | Some(PathComponent::Metadata)
                                | Some(PathComponent::Wildcard)
                        ) {
                            return Err(PathError::ValidationError(
                                "Resources must be followed by /d, /m, or /*".to_string(),
                            ));
                        }
                    }
                    PathComponent::Data => {
                        // Data must be followed by Bucket or Wildcard
                        if !matches!(
                            next,
                            Some(PathComponent::Bucket(_)) | Some(PathComponent::Wildcard)
                        ) {
                            return Err(PathError::ValidationError(
                                "Data must be followed by a bucket name or /*".to_string(),
                            ));
                        }
                    }
                    PathComponent::Bucket(_) => {
                        // Bucket can be followed by Key or Wildcard
                        if !matches!(
                            next,
                            Some(PathComponent::Key(_)) | Some(PathComponent::Wildcard) | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Bucket must be followed by a key or /*".to_string(),
                            ));
                        }
                    }
                    PathComponent::Key(_) => {
                        // Key can be followed by ContentHash or Wildcard
                        if !matches!(
                            next,
                            Some(PathComponent::ContentHash(_))
                                | Some(PathComponent::Wildcard)
                                | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Key must be followed by a content hash or /*".to_string(),
                            ));
                        }
                    }
                    PathComponent::ContentHash(_) => {
                        // ContentHash can only be followed by Wildcard or nothing
                        if !matches!(next, Some(PathComponent::Wildcard) | None) {
                            return Err(PathError::ValidationError(
                                "Content Hash can only be followed by /* or nothing".to_string(),
                            ));
                        }
                    }
                    PathComponent::Metadata => {
                        // Metadata must be followed by ProjectId or Wildcard
                        if !matches!(
                            next,
                            Some(PathComponent::ProjectId(_)) | Some(PathComponent::Wildcard)
                        ) {
                            return Err(PathError::ValidationError(
                                "Metadata must be followed by a project ID or /*".to_string(),
                            ));
                        }
                    }
                    PathComponent::ProjectId(_) => {
                        // ProjectId can be followed by ObjectId, Wildcard, or nothing
                        if !matches!(
                            next,
                            Some(PathComponent::ObjectId(_)) | Some(PathComponent::Wildcard) | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Project ID can only be followed by an object ID, /*, or nothing"
                                    .to_string(),
                            ));
                        }
                    }
                    PathComponent::ObjectId(_) => {
                        // ObjectId can be followed by another ObjectId, Wildcard, or nothing
                        if !matches!(
                            next,
                            Some(PathComponent::ObjectId(_)) | Some(PathComponent::Wildcard) | None
                        ) {
                            return Err(PathError::ValidationError(
                                "Object ID can only be followed by another object ID, /*, or nothing".to_string(),
                            ));
                        }
                    }
                    PathComponent::Wildcard => {
                        // Wildcard must be the last component
                        if next.is_some() {
                            return Err(PathError::ValidationError(
                                "Wildcard must be the last component in the path".to_string(),
                            ));
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

/// Builder for creating Path instances.
#[derive(Default)]
pub struct PathBuilder {
    components: Vec<PathComponent>,
}

impl PathBuilder {
    /// Creates a new PathBuilder with default values.
    pub fn new() -> Self {
        Self {
            components: Vec::new(),
        }
    }

    /// Sets the realm ID for the path.
    pub fn realm_id(mut self, realm_id: Ulid) -> Self {
        self.components.push(PathComponent::RealmId(realm_id));
        self
    }

    /// Adds the admin section to the path.
    pub fn admin(mut self) -> Self {
        self.components.push(PathComponent::Admin);
        self
    }

    /// Adds the admin groups section to the path.
    pub fn admin_groups(mut self) -> Self {
        self.components.push(PathComponent::Admin);
        self.components.push(PathComponent::Groups);
        self
    }

    /// Adds the admin policies section to the path.
    pub fn admin_policies(mut self) -> Self {
        self.components.push(PathComponent::Admin);
        self.components.push(PathComponent::Policies);
        self
    }

    /// Adds a specific admin policy section to the path.
    pub fn admin_policy(mut self, policy_id: Ulid) -> Self {
        self.components.push(PathComponent::Admin);
        self.components.push(PathComponent::Policies);
        self.components.push(PathComponent::PolicyId(policy_id));
        self
    }

    /// Adds a wildcard to the current path.
    pub fn wildcard(mut self) -> Self {
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds the admin wildcard section to the path.
    pub fn admin_wildcard(mut self) -> Self {
        self.components.push(PathComponent::Admin);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds the admin groups wildcard section to the path.
    pub fn admin_groups_wildcard(mut self) -> Self {
        self.components.push(PathComponent::Admin);
        self.components.push(PathComponent::Groups);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds the admin policies wildcard section to the path.
    pub fn admin_policies_wildcard(mut self) -> Self {
        self.components.push(PathComponent::Admin);
        self.components.push(PathComponent::Policies);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group section to the path.
    pub fn group(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self
    }

    /// Adds a group admin section to the path.
    pub fn group_admin(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::GroupAdmin);
        self
    }

    /// Adds a group admin wildcard section to the path.
    pub fn group_admin_wildcard(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::GroupAdmin);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group wildcard section to the path.
    pub fn group_wildcard(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group data resource section to the path with bucket, key, and content hash.
    pub fn group_data(
        mut self,
        group_id: Ulid,
        bucket: String,
        key: String,
        content_hash: Blake3Hash,
    ) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Bucket(bucket));
        self.components.push(PathComponent::Key(key));
        self.components
            .push(PathComponent::ContentHash(content_hash));
        self
    }

    /// Adds a group data wildcard section to the path (all data resources).
    pub fn group_data_wildcard(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group data bucket wildcard section to the path (all keys in a specific bucket).
    pub fn group_data_bucket_wildcard(mut self, group_id: Ulid, bucket: String) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Bucket(bucket));
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group data bucket section to the path (all keys in a specific bucket).
    pub fn group_data_bucket(mut self, group_id: Ulid, bucket: String) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Bucket(bucket));
        self
    }

    /// Adds a group data key prefix wildcard section to the path (all keys with a prefix in a bucket).
    pub fn group_data_key_prefix_wildcard(
        mut self,
        group_id: Ulid,
        bucket: String,
        key_prefix: String,
    ) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Bucket(bucket));
        self.components.push(PathComponent::Key(key_prefix));
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group data key prefix section to the path (all keys with a prefix in a bucket).
    pub fn group_data_key_prefix(
        mut self,
        group_id: Ulid,
        bucket: String,
        key_prefix: String,
    ) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Bucket(bucket));
        self.components.push(PathComponent::Key(key_prefix));
        self
    }

    /// Adds a group resources wildcard section to the path.
    pub fn group_resources_wildcard(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group metadata resource section to the path.
    pub fn group_metadata(
        mut self,
        group_id: Ulid,
        project_id: Ulid,
        sub_object_ids: Vec<Ulid>,
    ) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Metadata);
        self.components.push(PathComponent::ProjectId(project_id));

        for object_id in sub_object_ids {
            self.components.push(PathComponent::ObjectId(object_id));
        }

        self
    }

    /// Adds a group metadata wildcard section to the path.
    pub fn group_metadata_wildcard(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Metadata);
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group specific project metadata wildcard section to the path.
    pub fn group_metadata_project_wildcard(mut self, group_id: Ulid, project_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Metadata);
        self.components.push(PathComponent::ProjectId(project_id));
        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Adds a group specific metadata path wildcard section to the path.
    pub fn group_metadata_specific_path_wildcard(
        mut self,
        group_id: Ulid,
        project_id: Ulid,
        sub_object_ids: Vec<Ulid>,
    ) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Metadata);
        self.components.push(PathComponent::ProjectId(project_id));

        for object_id in sub_object_ids {
            self.components.push(PathComponent::ObjectId(object_id));
        }

        self.components.push(PathComponent::Wildcard);
        self
    }

    /// Builds the path, ensuring all components form a valid path.
    pub fn build(self) -> Result<Path> {
        let path = Path {
            components: self.components,
        };

        path.validate()?;

        Ok(path)
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        for component in &self.components {
            match component {
                PathComponent::RealmId(ulid) => {
                    write!(f, "{}", ulid)?;
                }
                PathComponent::Admin => write!(f, "/a")?,
                PathComponent::Groups => write!(f, "/g")?,
                PathComponent::Policies => write!(f, "/p")?,
                PathComponent::PolicyId(policy_id) => write!(f, "/{}", policy_id)?,
                PathComponent::Group => write!(f, "/g")?,
                PathComponent::GroupId(group_id) => write!(f, "/{}", group_id)?,
                PathComponent::GroupAdmin => write!(f, "/a")?,
                PathComponent::Resources => write!(f, "/r")?,
                PathComponent::Data => write!(f, "/d")?,
                PathComponent::Bucket(bucket) => write!(f, "/{}", bucket)?,
                PathComponent::Key(key) => write!(f, "/{}", key)?,
                PathComponent::ContentHash(content_hash) => {
                    write!(f, "#{}", content_hash.to_hex())?;
                }
                PathComponent::Metadata => write!(f, "/m")?,
                PathComponent::ProjectId(project_id) => write!(f, "/{}", project_id)?,
                PathComponent::ObjectId(object_id) => write!(f, "/{}", object_id)?,
                PathComponent::Wildcard => write!(f, "/*")?,
            }
        }

        Ok(())
    }
}

impl Into<Vec<u8>> for &Path {
    fn into(self) -> Vec<u8> {
        self.to_string().into()
    }
}

impl TryFrom<&[u8]> for Path {
    type Error = PathError;

    fn try_from(value: &[u8]) -> Result<Self> {
        let input = std::str::from_utf8(value)
            .map_err(|_| PathError::ParseError("Invalid UTF-8 in path".to_string()))?;
        Self::parse(input)
    }
}

// Parser implementation using nom

/// Validates if a character is valid for a ULID.
fn is_valid_ulid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() && !c.is_ascii_lowercase()
}

/// Validates if a character is valid for bucket names.
/// Bucket names follow S3 naming rules: alphanumeric, hyphens, underscores, dots.
fn is_valid_bucket_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.'
}

/// Validates if a character is valid for S3 key names.
/// S3 keys can contain most printable ASCII characters except the delimiter '#'
fn is_valid_key_char(c: char) -> bool {
    // Allow most printable ASCII characters except our delimiter '#'
    c.is_ascii() && c.is_ascii_graphic() && c != '#'
}

/// Parses a ULID.
fn ulid_parser(input: &str) -> IResult<&str, Ulid> {
    let (input, ulid_str) = take_while1(is_valid_ulid_char).parse(input)?;

    match Ulid::from_str(ulid_str) {
        Ok(ulid) => Ok((input, ulid)),
        Err(_) => Err(NomErr::Error(NomError::new(input, ErrorKind::AlphaNumeric))),
    }
}

/// Parses a bucket name (no slashes allowed).
fn bucket_parser(input: &str) -> IResult<&str, String> {
    let (input, bucket_str) = take_while1(is_valid_bucket_char).parse(input)?;
    Ok((input, bucket_str.to_string()))
}

/// Parses a key name that can contain slashes and other characters.
/// Stops at '#' delimiter or wildcard '/*'.
fn key_parser(input: &str) -> IResult<&str, String> {
    // Check for wildcard first
    if input.starts_with("/*") {
        return Err(NomErr::Error(NomError::new(input, ErrorKind::Tag)));
    }

    // Parse key until we hit '#' delimiter or end of input
    let (input, key_str) = take_while1(is_valid_key_char).parse(input)?;

    // Edge case: if the key ends with '/*', we treat it as a wildcard
    if key_str.ends_with("/*") {
        return Ok(("/*", key_str[..key_str.len() - 2].to_string()));
    }

    Ok((input, key_str.to_string()))
}

/// Parses the delimiter '#' followed by a base64-encoded Blake3 hash.
fn content_hash_delimiter_parser(input: &str) -> IResult<&str, Blake3Hash> {
    let (input, _) = tag("#").parse(input)?;
    let (input, hash_str) =
        take_while1(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_').parse(input)?;

    let hash = Blake3Hash::from_str(hash_str)
        .map_err(|_| NomErr::Error(NomError::new(input, ErrorKind::AlphaNumeric)))?;

    Ok((input, hash))
}

/// Parser for a wildcard component.
fn wildcard_parser(input: &str) -> IResult<&str, PathComponent> {
    let (input, _) = tag("/*").parse(input)?;
    Ok((input, PathComponent::Wildcard))
}

/// Parses a complete path.
fn path_parser(input: &str) -> IResult<&str, Vec<PathComponent>> {
    let (input, realm_id) = ulid_parser(input)?;
    let mut components = vec![PathComponent::RealmId(realm_id)];

    let mut remaining = input;

    // If there's nothing after the realm ID, return just the realm ID component
    if remaining.is_empty() {
        return Ok((remaining, components));
    }

    // Parse the next section - must be either /a or /g
    let (input, section) = alt((
        map(tag("/a"), |_| PathComponent::Admin),
        map(tag("/g"), |_| PathComponent::Group),
    ))
    .parse(remaining)?;

    components.push(section.clone());
    remaining = input;

    // Parse the rest of the path based on the section
    match section {
        PathComponent::Admin => {
            // Parse admin path
            if remaining.is_empty() {
                return Ok((remaining, components));
            }

            // Check for wildcard immediately after /a
            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                components.push(wildcard);
                return Ok((input, components));
            }

            // Otherwise, parse admin subsection
            let (input, subsection) = alt((
                map(tag("/g"), |_| PathComponent::Groups),
                map(tag("/p"), |_| PathComponent::Policies),
            ))
            .parse(remaining)?;

            components.push(subsection.clone());
            remaining = input;

            match subsection {
                PathComponent::Groups => {
                    // After /a/g we can only have /* or nothing
                    if remaining.is_empty() {
                        return Ok((remaining, components));
                    }

                    let (input, wildcard) = wildcard_parser.parse(remaining)?;
                    components.push(wildcard);
                    return Ok((input, components));
                }
                PathComponent::Policies => {
                    // After /a/p we can have a policy ID, /* or nothing
                    if remaining.is_empty() {
                        return Ok((remaining, components));
                    }

                    if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                        components.push(wildcard);
                        return Ok((input, components));
                    }

                    let (input, _) = tag("/").parse(remaining)?;
                    let (input, policy_id) = ulid_parser(input)?;
                    components.push(PathComponent::PolicyId(policy_id));

                    remaining = input;

                    // After policy ID, we can have /* or nothing
                    if remaining.is_empty() {
                        return Ok((remaining, components));
                    }

                    let (input, wildcard) = wildcard_parser.parse(remaining)?;
                    components.push(wildcard);
                    return Ok((input, components));
                }
                _ => unreachable!(),
            }
        }
        PathComponent::Group => {
            // Group must be followed by a group ID
            let (input, _) = tag("/").parse(remaining)?;
            let (input, group_id) = ulid_parser(input)?;
            components.push(PathComponent::GroupId(group_id));

            remaining = input;

            // After group ID we can have /a, /r, /*, or nothing
            if remaining.is_empty() {
                return Ok((remaining, components));
            }

            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                components.push(wildcard);
                return Ok((input, components));
            }

            let (input, subsection) = alt((
                map(tag("/a"), |_| PathComponent::GroupAdmin),
                map(tag("/r"), |_| PathComponent::Resources),
            ))
            .parse(remaining)?;

            components.push(subsection.clone());
            remaining = input;

            match subsection {
                PathComponent::GroupAdmin => {
                    // After /g/{group_id}/a we can have /* or nothing
                    if remaining.is_empty() {
                        return Ok((remaining, components));
                    }

                    let (input, wildcard) = wildcard_parser.parse(remaining)?;
                    components.push(wildcard);
                    return Ok((input, components));
                }
                PathComponent::Resources => {
                    // After /g/{group_id}/r we must have /d, /m, or /*
                    if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                        components.push(wildcard);
                        return Ok((input, components));
                    }

                    let (input, resource_type) = alt((
                        map(tag("/d"), |_| PathComponent::Data),
                        map(tag("/m"), |_| PathComponent::Metadata),
                    ))
                    .parse(remaining)?;

                    components.push(resource_type.clone());
                    remaining = input;

                    match resource_type {
                        PathComponent::Data => {
                            // After /r/d we can have a bucket or /*
                            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                components.push(wildcard);
                                return Ok((input, components));
                            }

                            let (input, _) = tag("/").parse(remaining)?;
                            let (input, bucket) = bucket_parser(input)?;
                            components.push(PathComponent::Bucket(bucket));

                            remaining = input;

                            // After bucket we can have a key or /*
                            if remaining.is_empty() {
                                return Ok((remaining, components));
                            }

                            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                components.push(wildcard);
                                return Ok((input, components));
                            }

                            let (input, _) = tag("/").parse(remaining)?;
                            let (input, key) = key_parser(input)?;
                            components.push(PathComponent::Key(key));

                            remaining = input;

                            // After key we can have a content hash with delimiter or /*
                            if remaining.is_empty() {
                                return Ok((remaining, components));
                            }

                            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                components.push(wildcard);
                                return Ok((input, components));
                            }

                            // Parse content hash with '#' delimiter
                            let (input, hash) = content_hash_delimiter_parser(remaining)?;
                            components.push(PathComponent::ContentHash(hash));

                            remaining = input;

                            // After content hash we can have /* or nothing
                            if remaining.is_empty() {
                                return Ok((remaining, components));
                            }

                            let (input, wildcard) = wildcard_parser.parse(remaining)?;
                            components.push(wildcard);
                            return Ok((input, components));
                        }
                        PathComponent::Metadata => {
                            // After /r/m we can have a project ID or /*
                            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                components.push(wildcard);
                                return Ok((input, components));
                            }

                            let (input, _) = tag("/").parse(remaining)?;
                            let (input, project_id) = ulid_parser(input)?;
                            components.push(PathComponent::ProjectId(project_id));

                            remaining = input;

                            // After project ID we can have object IDs, /*, or nothing
                            if remaining.is_empty() {
                                return Ok((remaining, components));
                            }

                            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                components.push(wildcard);
                                return Ok((input, components));
                            }

                            // Parse a sequence of object IDs
                            while !remaining.is_empty() {
                                if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                    components.push(wildcard);
                                    return Ok((input, components));
                                }

                                let (input, _) = tag("/").parse(remaining)?;
                                let (input, object_id) = ulid_parser(input)?;
                                components.push(PathComponent::ObjectId(object_id));

                                remaining = input;

                                // After object ID we can have another object ID, /*, or nothing
                                if remaining.is_empty() {
                                    return Ok((remaining, components));
                                }
                            }

                            return Ok((remaining, components));
                        }
                        _ => unreachable!(),
                    }
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper function to create test ULIDs from strings
    fn create_test_ulid(s: &str) -> Ulid {
        // Use a deterministic ULID for tests - this assumes valid strings!
        Ulid::from_str(s).unwrap_or_else(|_| Ulid::new())
    }

    // Helper function to create a test Blake3 hash
    fn create_test_hash(s: &str) -> Blake3Hash {
        // Create a deterministic hash for tests
        let hash = blake3::hash(s.as_bytes());
        hash
    }

    #[test]
    fn test_parse_realm_only() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = realm_id.to_string();

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 1);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_admin_root() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 2);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_admin_groups() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/g", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 3);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Groups);
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_admin_policies() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/p", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 3);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Policies);
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_admin_policy() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let path_str = format!("{}/a/p/{}", realm_id, policy_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 4);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Policies);
        assert_eq!(path.components()[3], PathComponent::PolicyId(policy_id));
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_group() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 3);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_group_admin() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/a", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 4);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::GroupAdmin);
        assert!(!path.has_wildcards());
    }

    // New tests for bucket/key functionality
    #[test]
    fn test_parse_group_data_with_bucket_key_hash() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket";
        let key = "documents/file.txt";
        let hash = create_test_hash("test-content");
        let encoded_hash = hash.to_hex();
        let path_str = format!(
            "{}/g/{}/r/d/{}/{}#{}",
            realm_id, group_id, bucket, key, encoded_hash
        );

        dbg!(&path_str);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 8);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Data);
        assert_eq!(
            path.components()[5],
            PathComponent::Bucket(bucket.to_string())
        );
        assert_eq!(path.components()[6], PathComponent::Key(key.to_string()));
        assert_eq!(path.components()[7], PathComponent::ContentHash(hash));
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_parse_group_data_wildcard() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/r/d/*", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 6);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Data);
        assert_eq!(path.components()[5], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_parse_group_data_bucket_wildcard() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket";
        let path_str = format!("{}/g/{}/r/d/{}/*", realm_id, group_id, bucket);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 7);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Data);
        assert_eq!(
            path.components()[5],
            PathComponent::Bucket(bucket.to_string())
        );
        assert_eq!(path.components()[6], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_parse_group_data_key_prefix_wildcard() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket";
        let key_prefix = "documents";
        let path_str = format!(
            "{}/g/{}/r/d/{}/{}/*",
            realm_id, group_id, bucket, key_prefix
        );

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 8);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Data);
        assert_eq!(
            path.components()[5],
            PathComponent::Bucket(bucket.to_string())
        );
        assert_eq!(
            path.components()[6],
            PathComponent::Key(key_prefix.to_string())
        );
        assert_eq!(path.components()[7], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_parse_group_metadata() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let folder_id1 = create_test_ulid("01H1VECTFR4444444444444444");
        let folder_id2 = create_test_ulid("01H1VECTFR5555555555555555");
        let object_id = create_test_ulid("01H1VECTFR6666666666666666");

        let path_str = format!(
            "{}/g/{}/r/m/{}/{}/{}/{}",
            realm_id, group_id, project_id, folder_id1, folder_id2, object_id
        );

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 9);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Metadata);
        assert_eq!(path.components()[5], PathComponent::ProjectId(project_id));
        assert_eq!(path.components()[6], PathComponent::ObjectId(folder_id1));
        assert_eq!(path.components()[7], PathComponent::ObjectId(folder_id2));
        assert_eq!(path.components()[8], PathComponent::ObjectId(object_id));
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_wildcard_admin() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/*", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 3);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_admin_groups() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/g/*", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 4);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Groups);
        assert_eq!(path.components()[3], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_admin_policies() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/p/*", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 4);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Policies);
        assert_eq!(path.components()[3], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_policy() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let path_str = format!("{}/a/p/{}/*", realm_id, policy_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 5);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Policies);
        assert_eq!(path.components()[3], PathComponent::PolicyId(policy_id));
        assert_eq!(path.components()[4], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_realm_admin() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/*", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 3);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Admin);
        assert_eq!(path.components()[2], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_group() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/*", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 4);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_group_admin() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/a/*", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 5);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::GroupAdmin);
        assert_eq!(path.components()[4], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_resources() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/r/*", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 5);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_metadata() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/r/m/*", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 6);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Metadata);
        assert_eq!(path.components()[5], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_project() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let path_str = format!("{}/g/{}/r/m/{}/*", realm_id, group_id, project_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 7);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Metadata);
        assert_eq!(path.components()[5], PathComponent::ProjectId(project_id));
        assert_eq!(path.components()[6], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_wildcard_object() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let folder_id = create_test_ulid("01H1VECTFR4444444444444444");
        let path_str = format!(
            "{}/g/{}/r/m/{}/{}/*",
            realm_id, group_id, project_id, folder_id
        );

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 8);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Metadata);
        assert_eq!(path.components()[5], PathComponent::ProjectId(project_id));
        assert_eq!(path.components()[6], PathComponent::ObjectId(folder_id));
        assert_eq!(path.components()[7], PathComponent::Wildcard);
        assert!(path.has_wildcards());
    }

    // New builder tests for bucket/key functionality
    #[test]
    fn test_builder_group_data_with_bucket_key() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket".to_string();
        let key = "documents/file.txt".to_string();
        let hash = create_test_hash("test-content");

        let path = Path::builder()
            .realm_id(realm_id)
            .group_data(group_id, bucket.clone(), key.clone(), hash)
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/d/{}/{}#{}",
                realm_id,
                group_id,
                bucket,
                key,
                hash.to_hex()
            )
        );
        assert!(!path.has_wildcards());
    }

    #[test]
    fn test_builder_group_data_wildcards() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket".to_string();
        let key_prefix = "documents".to_string();

        // Test data wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data_wildcard(group_id)
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/*", realm_id, group_id)
        );
        assert!(path.has_wildcards());

        // Test bucket wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data_bucket_wildcard(group_id, bucket.clone())
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/{}/*", realm_id, group_id, bucket)
        );
        assert!(path.has_wildcards());

        // Test key prefix wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data_key_prefix_wildcard(group_id, bucket.clone(), key_prefix.clone())
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/d/{}/{}/*",
                realm_id, group_id, bucket, key_prefix
            )
        );
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_path_validation_bucket_key() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");

        // Test invalid path with bucket but no key and no wildcard (this is now valid)
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Group,
            PathComponent::GroupId(group_id),
            PathComponent::Resources,
            PathComponent::Data,
            PathComponent::Bucket("my-bucket".to_string()),
        ];

        let path = Path { components };
        assert!(path.validate().is_ok());

        // Test invalid path with key but no content hash and no wildcard (this is now valid)
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Group,
            PathComponent::GroupId(group_id),
            PathComponent::Resources,
            PathComponent::Data,
            PathComponent::Bucket("my-bucket".to_string()),
            PathComponent::Key("my-key".to_string()),
        ];

        let path = Path { components };
        assert!(path.validate().is_ok());

        // Test valid path with wildcard after data
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Group,
            PathComponent::GroupId(group_id),
            PathComponent::Resources,
            PathComponent::Data,
            PathComponent::Wildcard,
        ];

        let path = Path { components };
        assert!(path.validate().is_ok());

        // Test valid path with wildcard after bucket
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Group,
            PathComponent::GroupId(group_id),
            PathComponent::Resources,
            PathComponent::Data,
            PathComponent::Bucket("my-bucket".to_string()),
            PathComponent::Wildcard,
        ];

        let path = Path { components };
        assert!(path.validate().is_ok());

        // Test valid path with wildcard after key
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Group,
            PathComponent::GroupId(group_id),
            PathComponent::Resources,
            PathComponent::Data,
            PathComponent::Bucket("my-bucket".to_string()),
            PathComponent::Key("my-key".to_string()),
            PathComponent::Wildcard,
        ];

        let path = Path { components };
        assert!(path.validate().is_ok());
    }

    #[test]
    fn test_to_string_with_bucket_key() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket".to_string();
        let key = "documents/file.txt".to_string();
        let hash = create_test_hash("test-content");

        // Test complete data path with bucket/key/hash
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data(group_id, bucket.clone(), key.clone(), hash)
            .build()
            .unwrap();
        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/d/{}/{}#{}",
                realm_id,
                group_id,
                bucket,
                key,
                hash.to_hex()
            )
        );

        // Test data wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data_wildcard(group_id)
            .build()
            .unwrap();
        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/*", realm_id, group_id)
        );

        // Test bucket wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data_bucket_wildcard(group_id, bucket.clone())
            .build()
            .unwrap();
        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/{}/*", realm_id, group_id, bucket)
        );

        // Test key prefix wildcard
        let key_prefix = "docs".to_string();
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data_key_prefix_wildcard(group_id, bucket.clone(), key_prefix.clone())
            .build()
            .unwrap();
        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/d/{}/{}/*",
                realm_id, group_id, bucket, key_prefix
            )
        );
    }

    #[test]
    fn test_new_format_with_slashes_in_keys() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let bucket = "my-bucket";
        let key = "folder1/folder2/file.txt"; // Key with slashes
        let hash = create_test_hash("test-content");
        let encoded_hash = hash.to_hex();

        // New format: bucket/key#hash
        let path_str = format!(
            "{}/g/{}/r/d/{}/{}#{}",
            realm_id, group_id, bucket, key, encoded_hash
        );

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 8);
        assert_eq!(
            path.components()[5],
            PathComponent::Bucket(bucket.to_string())
        );
        assert_eq!(path.components()[6], PathComponent::Key(key.to_string()));
        assert_eq!(path.components()[7], PathComponent::ContentHash(hash));
    }

    #[test]
    fn test_edge_cases() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");

        // Case 1: <bucket>/key_part1/keypart2 (key with slashes, no hash)
        // This would be: realm/g/group/r/d/bucket/key_part1/keypart2/*
        let path_str = format!("{}/g/{}/r/d/my-bucket/docs/subfolder/*", realm_id, group_id);
        let path = Path::parse(&path_str).unwrap();
        assert!(path.has_wildcards());

        // Case 2: <bucket>/* (bucket with wildcard)
        let path_str = format!("{}/g/{}/r/d/my-bucket/*", realm_id, group_id);
        let path = Path::parse(&path_str).unwrap();
        assert!(path.has_wildcards());

        // Case 3: <bucket>/keypart1/keypart2/* (bucket with key wildcard)
        let path_str = format!("{}/g/{}/r/d/my-bucket/docs/files/*", realm_id, group_id);
        let path = Path::parse(&path_str).unwrap();
        assert!(path.has_wildcards());

        // Case 4: <bucket>/keypart1/keypart2#hash (bucket and key with content hash)
        let hash = create_test_hash("test");
        let encoded_hash = hash.to_hex();
        let path_str = format!(
            "{}/g/{}/r/d/my-bucket/docs/file.txt#{}",
            realm_id, group_id, encoded_hash
        );
        let path = Path::parse(&path_str).unwrap();
        assert!(!path.has_wildcards());
        assert_eq!(
            path.components()[6],
            PathComponent::Key("docs/file.txt".to_string())
        );
        assert_eq!(path.components()[7], PathComponent::ContentHash(hash));
    }
}
