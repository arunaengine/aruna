use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use blake3::Hash as Blake3Hash;
use nom::{
    Err as NomErr, IResult, Parser,
    branch::alt,
    bytes::complete::{tag, take_while1},
    combinator::{map, opt},
    error::{Error as NomError, ErrorKind},
    multi::many0,
    sequence::preceded,
};
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use thiserror::Error;
use ulid::Ulid;

/// Custom error type for path operations.
#[derive(Error, Debug)]
pub enum PathError {
    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Invalid ULID: {0}")]
    InvalidUlid(#[from] ulid::DecodeError),

    #[error("Invalid Blake3 hash: {0}")]
    InvalidHash(String),

    #[error("Invalid base64: {0}")]
    InvalidBase64(#[from] base64::DecodeError),

    #[error("Validation error: {0}")]
    ValidationError(String),

    #[error("Building error: {0}")]
    BuildError(String),
}

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
                        // Data must be followed by ContentHash or Wildcard
                        if !matches!(
                            next,
                            Some(PathComponent::ContentHash(_)) | Some(PathComponent::Wildcard)
                        ) {
                            return Err(PathError::ValidationError(
                                "Data must be followed by a content hash or /*".to_string(),
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

    /// Adds a group data resource section to the path.
    pub fn group_data(mut self, group_id: Ulid, content_hash: Blake3Hash) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components
            .push(PathComponent::ContentHash(content_hash));
        self
    }

    /// Adds a group data wildcard section to the path.
    pub fn group_data_wildcard(mut self, group_id: Ulid) -> Self {
        self.components.push(PathComponent::Group);
        self.components.push(PathComponent::GroupId(group_id));
        self.components.push(PathComponent::Resources);
        self.components.push(PathComponent::Data);
        self.components.push(PathComponent::Wildcard);
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
                PathComponent::ContentHash(content_hash) => {
                    let encoded = URL_SAFE_NO_PAD.encode(content_hash.as_bytes());
                    write!(f, "/{}", encoded)?;
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

// Parser implementation using nom

/// Validates if a character is valid for a ULID.
fn is_valid_ulid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() && !c.is_ascii_lowercase()
}

/// Parses a ULID.
fn ulid_parser(input: &str) -> IResult<&str, Ulid> {
    let (input, ulid_str) = take_while1(is_valid_ulid_char).parse(input)?;

    match Ulid::from_str(ulid_str) {
        Ok(ulid) => Ok((input, ulid)),
        Err(_) => Err(NomErr::Error(NomError::new(input, ErrorKind::AlphaNumeric))),
    }
}

/// Parses a base64 encoded Blake3 hash.
fn blake3_hash_parser(input: &str) -> IResult<&str, Blake3Hash> {
    let (input, hash_str) =
        take_while1(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_').parse(input)?;

    match URL_SAFE_NO_PAD.decode(hash_str) {
        Ok(bytes) => {
            if bytes.len() == 32 {
                let mut hash_bytes = [0u8; 32];
                hash_bytes.copy_from_slice(&bytes);
                Ok((input, Blake3Hash::from_bytes(hash_bytes)))
            } else {
                Err(NomErr::Error(NomError::new(input, ErrorKind::LengthValue)))
            }
        }
        Err(_) => Err(NomErr::Error(NomError::new(input, ErrorKind::AlphaNumeric))),
    }
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
                            // After /r/d we can have a content hash or /*
                            if let Ok((input, wildcard)) = wildcard_parser.parse(remaining) {
                                components.push(wildcard);
                                return Ok((input, components));
                            }

                            let (input, _) = tag("/").parse(remaining)?;
                            let (input, hash) = blake3_hash_parser(input)?;
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

    // Helper function to encode a Blake3 hash as base64
    fn encode_hash(hash: &Blake3Hash) -> String {
        URL_SAFE_NO_PAD.encode(hash.as_bytes())
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

    #[test]
    fn test_parse_group_data() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let hash = create_test_hash("test-content");
        let encoded_hash = encode_hash(&hash);
        let path_str = format!("{}/g/{}/r/d/{}", realm_id, group_id, encoded_hash);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), Some(realm_id));
        assert_eq!(path.components().len(), 6);
        assert_eq!(path.components()[0], PathComponent::RealmId(realm_id));
        assert_eq!(path.components()[1], PathComponent::Group);
        assert_eq!(path.components()[2], PathComponent::GroupId(group_id));
        assert_eq!(path.components()[3], PathComponent::Resources);
        assert_eq!(path.components()[4], PathComponent::Data);
        assert_eq!(path.components()[5], PathComponent::ContentHash(hash));
        assert!(!path.has_wildcards());
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
    fn test_wildcard_data() {
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

    #[test]
    fn test_builder_admin_wildcards() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");

        // Test admin wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .admin()
            .wildcard()
            .build()
            .unwrap();

        assert_eq!(path.to_string(), format!("{}/a/*", realm_id));
        assert!(path.has_wildcards());

        // Test admin groups wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_groups()
            .wildcard()
            .build()
            .unwrap();

        assert_eq!(path.to_string(), format!("{}/a/g/*", realm_id));
        assert!(path.has_wildcards());

        // Test admin policies wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_policies()
            .wildcard()
            .build()
            .unwrap();

        assert_eq!(path.to_string(), format!("{}/a/p/*", realm_id));
        assert!(path.has_wildcards());

        // Test admin policy wildcard
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_policy(policy_id)
            .wildcard()
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!("{}/a/p/{}/*", realm_id, policy_id)
        );
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_builder_group_wildcards() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");

        // Test group wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group(group_id)
            .wildcard()
            .build()
            .unwrap();

        assert_eq!(path.to_string(), format!("{}/g/{}/*", realm_id, group_id));
        assert!(path.has_wildcards());

        // Test group admin wildcard
        let path = Path::builder()
            .realm_id(realm_id)
            .group_admin(group_id)
            .wildcard()
            .build()
            .unwrap();

        assert_eq!(path.to_string(), format!("{}/g/{}/a/*", realm_id, group_id));
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_builder_resource_wildcards() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");

        // Test resources wildcard
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Group,
            PathComponent::GroupId(group_id),
            PathComponent::Resources,
            PathComponent::Wildcard,
        ];

        let path = Path { components };

        assert_eq!(path.to_string(), format!("{}/g/{}/r/*", realm_id, group_id));
        assert!(path.has_wildcards());

        // Test data wildcard using the builder's specialized methods
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

        // Test metadata wildcard using the builder's specialized methods
        let path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_wildcard(group_id)
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/m/*", realm_id, group_id)
        );
        assert!(path.has_wildcards());

        // Test project wildcard using the builder's specialized methods
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_project_wildcard(group_id, project_id)
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/m/{}/*", realm_id, group_id, project_id)
        );
        assert!(path.has_wildcards());

        // Test metadata path wildcard using the builder's specialized methods
        let object_id = create_test_ulid("01H1VECTFR4444444444444444");
        let path = Path::builder()
            .realm_id(realm_id)
            .group_metadata_specific_path_wildcard(group_id, project_id, vec![object_id])
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/m/{}/{}/*",
                realm_id, group_id, project_id, object_id
            )
        );
        assert!(path.has_wildcards());
    }

    #[test]
    fn test_path_validation() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");

        // Test invalid path with wildcard not at the end
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Admin,
            PathComponent::Wildcard,
            PathComponent::Groups, // Invalid: component after wildcard
        ];

        let path = Path { components };
        assert!(path.validate().is_err());

        // Test invalid path with incorrect sequence
        let components = vec![
            PathComponent::RealmId(realm_id),
            PathComponent::Admin,
            PathComponent::Data, // Invalid: Data not valid after Admin
        ];

        let path = Path { components };
        assert!(path.validate().is_err());

        // Test invalid path starting with Group (no RealmId)
        let components = vec![
            PathComponent::Group, // Invalid: path must start with RealmId
            PathComponent::GroupId(group_id),
        ];

        let path = Path { components };
        assert!(path.validate().is_err());
    }

    #[test]
    fn test_to_string() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let folder_id = create_test_ulid("01H1VECTFR4444444444444444");
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let hash = create_test_hash("test-content");

        // Test realm only path
        let path = Path::builder().realm_id(realm_id).build().unwrap();
        assert_eq!(path.to_string(), realm_id.to_string());

        // Test admin path
        let path = Path::builder().realm_id(realm_id).admin().build().unwrap();
        assert_eq!(path.to_string(), format!("{}/a", realm_id));

        // Test admin groups path
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_groups()
            .build()
            .unwrap();
        assert_eq!(path.to_string(), format!("{}/a/g", realm_id));

        // Test admin policies path
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_policies()
            .build()
            .unwrap();
        assert_eq!(path.to_string(), format!("{}/a/p", realm_id));

        // Test admin policy path
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_policy(policy_id)
            .build()
            .unwrap();
        assert_eq!(path.to_string(), format!("{}/a/p/{}", realm_id, policy_id));

        // Test group path
        let path = Path::builder()
            .realm_id(realm_id)
            .group(group_id)
            .build()
            .unwrap();
        assert_eq!(path.to_string(), format!("{}/g/{}", realm_id, group_id));

        // Test group admin path
        let path = Path::builder()
            .realm_id(realm_id)
            .group_admin(group_id)
            .build()
            .unwrap();
        assert_eq!(path.to_string(), format!("{}/g/{}/a", realm_id, group_id));

        // Test group data path
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data(group_id, hash)
            .build()
            .unwrap();
        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/{}", realm_id, group_id, encode_hash(&hash))
        );

        // Test group metadata path
        let path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(group_id, project_id, vec![folder_id])
            .build()
            .unwrap();
        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/m/{}/{}",
                realm_id, group_id, project_id, folder_id
            )
        );
    }
}
