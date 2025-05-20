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

/// Represents a path in the custom path system.
/// Paths start with a realm ID and can have various subsections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path {
    realm_id: Ulid,
    section: Section,
}

/// Represents the high-level section of a path.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum Section {
    /// Just the realm, no additional path components
    #[default]
    Root,
    /// Administrative section: `/a` followed by optional subsection
    Admin(AdminSection),
    /// Group section: `/g/<groupid>` followed by optional subsection
    Group {
        group_id: Ulid,
        subsection: Option<GroupSubsection>,
    },
}

/// Represents the administrative subsections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminSection {
    /// Base admin section: `/a`
    Root,
    /// Groups management: `/a/g`
    Groups,
    /// Policies management: `/a/p`
    Policies,
    /// Specific policy management: `/a/p/<policyid>`
    Policy(Ulid),
}

/// Represents the group subsections.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GroupSubsection {
    /// Group admin: `/g/<groupid>/a`
    Admin,
    /// Group resources: Either data or metadata
    Resources(ResourceType),
}

/// Represents the types of resources in a group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResourceType {
    /// Data resources: `/r/d/<content-hash>`
    Data(Blake3Hash),
    /// Metadata resources: `/r/m/<projectid>/<folderid>[0-X]` The last might be an "object ID"
    Metadata {
        project_id: Ulid,
        sub_object_ids: Vec<Ulid>,
    },
}

impl Path {
    /// Returns the realm ID of the path.
    pub fn realm_id(&self) -> Ulid {
        self.realm_id
    }

    /// Returns the section part of the path.
    pub fn section(&self) -> &Section {
        &self.section
    }

    /// Returns a new path builder.
    pub fn builder() -> PathBuilder {
        PathBuilder::new()
    }

    /// Parses a string into a Path.
    pub fn parse(input: &str) -> Result<Self> {
        match path_parser(input) {
            Ok((remaining, path)) => {
                if remaining.is_empty() {
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
        // All validation is now done during parsing and building
        Ok(())
    }
}

/// Builder for creating Path instances.
#[derive(Default)]
pub struct PathBuilder {
    realm_id: Option<Ulid>,
    section: Section,
}

impl PathBuilder {
    /// Creates a new PathBuilder with default values.
    pub fn new() -> Self {
        Self {
            realm_id: None,
            section: Section::Root,
        }
    }

    /// Sets the realm ID for the path.
    pub fn realm_id(mut self, realm_id: Ulid) -> Self {
        self.realm_id = Some(realm_id);
        self
    }

    /// Sets the path to the admin root section.
    pub fn admin(mut self) -> Self {
        self.section = Section::Admin(AdminSection::Root);
        self
    }

    /// Sets the path to the admin groups section.
    pub fn admin_groups(mut self) -> Self {
        self.section = Section::Admin(AdminSection::Groups);
        self
    }

    /// Sets the path to the admin policies section.
    pub fn admin_policies(mut self) -> Self {
        self.section = Section::Admin(AdminSection::Policies);
        self
    }

    /// Sets the path to a specific admin policy section.
    pub fn admin_policy(mut self, policy_id: Ulid) -> Self {
        self.section = Section::Admin(AdminSection::Policy(policy_id));
        self
    }

    /// Sets the path to a group section.
    pub fn group(mut self, group_id: Ulid) -> Self {
        self.section = Section::Group {
            group_id,
            subsection: None,
        };
        self
    }

    /// Sets the path to a group admin section.
    pub fn group_admin(mut self, group_id: Ulid) -> Self {
        self.section = Section::Group {
            group_id,
            subsection: Some(GroupSubsection::Admin),
        };
        self
    }

    /// Sets the path to a group data resource section.
    pub fn group_data(mut self, group_id: Ulid, content_hash: Blake3Hash) -> Self {
        self.section = Section::Group {
            group_id,
            subsection: Some(GroupSubsection::Resources(ResourceType::Data(content_hash))),
        };
        self
    }

    /// Sets the path to a group metadata resource section.
    pub fn group_metadata(
        mut self,
        group_id: Ulid,
        project_id: Ulid,
        sub_object_ids: Vec<Ulid>,
    ) -> Self {
        self.section = Section::Group {
            group_id,
            subsection: Some(GroupSubsection::Resources(ResourceType::Metadata {
                project_id,
                sub_object_ids,
            })),
        };
        self
    }

    /// Builds the path, ensuring all required fields are provided.
    pub fn build(self) -> Result<Path> {
        let realm_id = self
            .realm_id
            .ok_or_else(|| PathError::BuildError("Realm ID is required".to_string()))?;

        let path = Path {
            realm_id,
            section: self.section,
        };

        path.validate()?;

        Ok(path)
    }
}

impl Display for Path {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.realm_id.to_string())?;
        match &self.section {
            Section::Root => Ok(()),
            Section::Admin(admin_section) => {
                write!(f, "/a")?;
                match admin_section {
                    AdminSection::Root => Ok(()),
                    AdminSection::Groups => write!(f, "/g"),
                    AdminSection::Policies => write!(f, "/p"),
                    AdminSection::Policy(policy_id) => write!(f, "/p/{}", policy_id),
                }
            }
            Section::Group {
                group_id,
                subsection,
            } => {
                write!(f, "/g/{}", group_id)?;

                if let Some(subsection) = subsection {
                    match subsection {
                        GroupSubsection::Admin => write!(f, "/a"),
                        GroupSubsection::Resources(resource_type) => match resource_type {
                            ResourceType::Data(content_hash) => {
                                let encoded = URL_SAFE_NO_PAD.encode(content_hash.as_bytes());
                                write!(f, "/r/d/{}", encoded)
                            }
                            ResourceType::Metadata {
                                project_id,
                                sub_object_ids,
                            } => {
                                write!(f, "/r/m/{}", project_id)?;

                                for sub_object in sub_object_ids {
                                    write!(f, "/{}", sub_object)?;
                                }

                                Ok(())
                            }
                        },
                    }
                } else {
                    Ok(())
                }
            }
        }
    }
}

// Parser implementation using nom

/// Validates if a character is valid for a ULID.
fn is_valid_ulid_char(c: char) -> bool {
    c.is_ascii_alphanumeric() && !c.is_ascii_lowercase()
}

/// Parses a ULID.
fn ulid_parser(input: &str) -> IResult<&str, Ulid> {
    let (input, ulid_str) = take_while1(is_valid_ulid_char)(input)?;

    match Ulid::from_str(ulid_str) {
        Ok(ulid) => Ok((input, ulid)),
        Err(_) => Err(NomErr::Error(NomError::new(input, ErrorKind::AlphaNumeric))),
    }
}

/// Parses a base64 encoded Blake3 hash.
fn blake3_hash_parser(input: &str) -> IResult<&str, Blake3Hash> {
    let (input, hash_str) =
        take_while1(|c: char| c.is_ascii_alphanumeric() || c == '-' || c == '_')(input)?;

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

/// Parses the realm ID at the start of a path.
fn realm_id_parser(input: &str) -> IResult<&str, Ulid> {
    ulid_parser(input)
}

/// Parses the admin section of a path.
fn admin_section_parser(input: &str) -> IResult<&str, AdminSection> {
    let (input, _) = tag("/a").parse(input)?;

    if input.is_empty() {
        return Ok((input, AdminSection::Root));
    }

    alt((
        map(tag("/g"), |_| AdminSection::Groups),
        map(preceded(tag("/p/"), ulid_parser), |policy_id| {
            AdminSection::Policy(policy_id)
        }),
        map(tag("/p"), |_| AdminSection::Policies),
    ))
    .parse(input)
}

/// Parses the group subsection of a path.
fn group_subsection_parser(input: &str) -> IResult<&str, GroupSubsection> {
    alt((
        map(tag("/a"), |_| GroupSubsection::Admin),
        map(preceded(tag("/r/d/"), blake3_hash_parser), |content_hash| {
            GroupSubsection::Resources(ResourceType::Data(content_hash))
        }),
        map(
            (
                preceded(tag("/r/m/"), ulid_parser),
                many0(preceded(tag("/"), ulid_parser)),
            ),
            |(project_id, sub_object_ids)| {
                GroupSubsection::Resources(ResourceType::Metadata {
                    project_id,
                    sub_object_ids,
                })
            },
        ),
    ))
    .parse(input)
}

/// Parses the group section of a path.
fn group_section_parser(input: &str) -> IResult<&str, (Ulid, Option<GroupSubsection>)> {
    let (input, _) = tag("/g/").parse(input)?;
    let (input, group_id) = ulid_parser(input)?;

    if input.is_empty() {
        return Ok((input, (group_id, None)));
    }

    let (input, subsection) = opt(group_subsection_parser).parse(input)?;

    Ok((input, (group_id, subsection)))
}

/// Parses the section part of a path.
fn section_parser(input: &str) -> IResult<&str, Section> {
    if input.is_empty() {
        return Ok((input, Section::Root));
    }

    alt((
        map(admin_section_parser, |admin_section| {
            Section::Admin(admin_section)
        }),
        map(group_section_parser, |(group_id, subsection)| {
            Section::Group {
                group_id,
                subsection,
            }
        }),
    ))
    .parse(input)
}

/// Parses a complete path.
fn path_parser(input: &str) -> IResult<&str, Path> {
    let (input, realm_id) = realm_id_parser(input)?;
    let (input, section) = section_parser(input)?;

    Ok((input, Path { realm_id, section }))
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
        assert_eq!(path.realm_id(), realm_id);
        assert_eq!(path.section(), &Section::Root);
    }

    #[test]
    fn test_parse_admin_root() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), realm_id);
        assert_eq!(path.section(), &Section::Admin(AdminSection::Root));
    }

    #[test]
    fn test_parse_admin_groups() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/g", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), realm_id);
        assert_eq!(path.section(), &Section::Admin(AdminSection::Groups));
    }

    #[test]
    fn test_parse_admin_policies() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let path_str = format!("{}/a/p", realm_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), realm_id);
        assert_eq!(path.section(), &Section::Admin(AdminSection::Policies));
    }

    #[test]
    fn test_parse_admin_policy() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let path_str = format!("{}/a/p/{}", realm_id, policy_id);

        let path = Path::parse(&path_str).unwrap();
        assert_eq!(path.realm_id(), realm_id);
        assert_eq!(
            path.section(),
            &Section::Admin(AdminSection::Policy(policy_id))
        );
    }

    #[test]
    fn test_parse_group() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();

        match path.section() {
            Section::Group {
                group_id: parsed_group_id,
                subsection,
            } => {
                assert_eq!(parsed_group_id, &group_id);
                assert_eq!(subsection, &None);
            }
            _ => panic!("Expected Group section"),
        }
    }

    #[test]
    fn test_parse_group_admin() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let path_str = format!("{}/g/{}/a", realm_id, group_id);

        let path = Path::parse(&path_str).unwrap();

        match path.section() {
            Section::Group {
                group_id: parsed_group_id,
                subsection,
            } => {
                assert_eq!(parsed_group_id, &group_id);
                assert_eq!(subsection, &Some(GroupSubsection::Admin));
            }
            _ => panic!("Expected Group section"),
        }
    }

    #[test]
    fn test_parse_group_data() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let hash = create_test_hash("test-content");
        let encoded_hash = encode_hash(&hash);
        let path_str = format!("{}/g/{}/r/d/{}", realm_id, group_id, encoded_hash);

        let path = Path::parse(&path_str).unwrap();

        match path.section() {
            Section::Group {
                group_id: parsed_group_id,
                subsection,
            } => {
                assert_eq!(parsed_group_id, &group_id);

                match subsection {
                    Some(GroupSubsection::Resources(ResourceType::Data(content_hash))) => {
                        assert_eq!(content_hash, &hash);
                    }
                    _ => panic!("Expected Data resource type"),
                }
            }
            _ => panic!("Expected Group section"),
        }
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

        match path.section() {
            Section::Group {
                group_id: parsed_group_id,
                subsection,
            } => {
                assert_eq!(parsed_group_id, &group_id);

                match subsection {
                    Some(GroupSubsection::Resources(ResourceType::Metadata {
                        project_id: parsed_project_id,
                        sub_object_ids,
                    })) => {
                        assert_eq!(parsed_project_id, &project_id);
                        assert_eq!(sub_object_ids, &vec![folder_id1, folder_id2, object_id]);
                    }
                    _ => panic!("Expected Metadata resource type"),
                }
            }
            _ => panic!("Expected Group section"),
        }
    }

    #[test]
    fn test_parse_group_metadata_no_object() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let folder_id1 = create_test_ulid("01H1VECTFR4444444444444444");
        let folder_id2 = create_test_ulid("01H1VECTFR5555555555555555");

        let path_str = format!(
            "{}/g/{}/r/m/{}/{}/{}",
            realm_id, group_id, project_id, folder_id1, folder_id2
        );

        let path = Path::parse(&path_str).unwrap();

        match path.section() {
            Section::Group {
                group_id: parsed_group_id,
                subsection,
            } => {
                assert_eq!(parsed_group_id, &group_id);

                match subsection {
                    Some(GroupSubsection::Resources(ResourceType::Metadata {
                        project_id: parsed_project_id,
                        sub_object_ids,
                    })) => {
                        assert_eq!(parsed_project_id, &project_id);
                        assert_eq!(sub_object_ids, &vec![folder_id1, folder_id2]);
                    }
                    _ => panic!("Expected Metadata resource type"),
                }
            }
            _ => panic!("Expected Group section"),
        }
    }

    #[test]
    fn test_to_string() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let folder_id1 = create_test_ulid("01H1VECTFR4444444444444444");
        let folder_id2 = create_test_ulid("01H1VECTFR5555555555555555");
        let object_id = create_test_ulid("01H1VECTFR6666666666666666");
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let hash = create_test_hash("test-content");

        // Test realm only
        let path = Path {
            realm_id,
            section: Section::Root,
        };
        assert_eq!(path.to_string(), realm_id.to_string());

        // Test admin paths
        let path = Path {
            realm_id,
            section: Section::Admin(AdminSection::Root),
        };
        assert_eq!(path.to_string(), format!("{}/a", realm_id));

        let path = Path {
            realm_id,
            section: Section::Admin(AdminSection::Groups),
        };
        assert_eq!(path.to_string(), format!("{}/a/g", realm_id));

        let path = Path {
            realm_id,
            section: Section::Admin(AdminSection::Policies),
        };
        assert_eq!(path.to_string(), format!("{}/a/p", realm_id));

        let path = Path {
            realm_id,
            section: Section::Admin(AdminSection::Policy(policy_id)),
        };
        assert_eq!(path.to_string(), format!("{}/a/p/{}", realm_id, policy_id));

        // Test group paths
        let path = Path {
            realm_id,
            section: Section::Group {
                group_id,
                subsection: None,
            },
        };
        assert_eq!(path.to_string(), format!("{}/g/{}", realm_id, group_id));

        let path = Path {
            realm_id,
            section: Section::Group {
                group_id,
                subsection: Some(GroupSubsection::Admin),
            },
        };
        assert_eq!(path.to_string(), format!("{}/g/{}/a", realm_id, group_id));

        let path = Path {
            realm_id,
            section: Section::Group {
                group_id,
                subsection: Some(GroupSubsection::Resources(ResourceType::Data(hash.clone()))),
            },
        };
        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/{}", realm_id, group_id, encode_hash(&hash))
        );

        let path = Path {
            realm_id,
            section: Section::Group {
                group_id,
                subsection: Some(GroupSubsection::Resources(ResourceType::Metadata {
                    project_id,
                    sub_object_ids: vec![folder_id1, folder_id2, object_id],
                })),
            },
        };
        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/m/{}/{}/{}/{}",
                realm_id, group_id, project_id, folder_id1, folder_id2, object_id
            )
        );

        let path = Path {
            realm_id,
            section: Section::Group {
                group_id,
                subsection: Some(GroupSubsection::Resources(ResourceType::Metadata {
                    project_id,
                    sub_object_ids: vec![folder_id1, folder_id2],
                })),
            },
        };
        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/m/{}/{}/{}",
                realm_id, group_id, project_id, folder_id1, folder_id2
            )
        );
    }

    #[test]
    fn test_builder() {
        let realm_id = create_test_ulid("01H1VECTFR0000000000000000");
        let policy_id = create_test_ulid("01H1VECTFR1111111111111111");
        let group_id = create_test_ulid("01H1VECTFR2222222222222222");
        let project_id = create_test_ulid("01H1VECTFR3333333333333333");
        let folder_id1 = create_test_ulid("01H1VECTFR4444444444444444");
        let folder_id2 = create_test_ulid("01H1VECTFR5555555555555555");
        let object_id = create_test_ulid("01H1VECTFR6666666666666666");
        let hash = create_test_hash("test-content");

        // Test admin policy path
        let path = Path::builder()
            .realm_id(realm_id)
            .admin_policy(policy_id)
            .build()
            .unwrap();

        assert_eq!(path.to_string(), format!("{}/a/p/{}", realm_id, policy_id));

        // Test metadata path
        let path = Path::builder()
            .realm_id(realm_id)
            .group_metadata(
                group_id,
                project_id,
                vec![folder_id1, folder_id2, object_id],
            )
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!(
                "{}/g/{}/r/m/{}/{}/{}/{}",
                realm_id, group_id, project_id, folder_id1, folder_id2, object_id
            )
        );

        // Test data path
        let path = Path::builder()
            .realm_id(realm_id)
            .group_data(group_id, hash.clone())
            .build()
            .unwrap();

        assert_eq!(
            path.to_string(),
            format!("{}/g/{}/r/d/{}", realm_id, group_id, encode_hash(&hash))
        );
    }

    #[test]
    fn test_error_handling() {
        // Test invalid ULID
        let result = Path::parse("invalid-ulid/a");
        assert!(result.is_err());

        // Test missing realm ID in builder
        let result = Path::builder()
            .admin_policy(create_test_ulid("01H1VECTFR1111111111111111"))
            .build();

        assert!(result.is_err());

        // Test invalid hash encoding
        let result = Path::parse(&format!(
            "{}/g/{}/r/d/invalid-hash-encoding",
            create_test_ulid("01H1VECTFR0000000000000000"),
            create_test_ulid("01H1VECTFR2222222222222222")
        ));
        assert!(result.is_err());
    }
}
