use crate::models::models::RelationInfo;

pub struct Field {
    pub name: &'static str,
    pub index: u32,
}

pub mod field_names {
    pub const ID_FIELD: &str = "id";
    pub const VARIANT_FIELD: &str = "variant";
    pub const DELETED_FIELD: &str = "deleted";
    pub const NAME_FIELD: &str = "name";
    pub const DESCRIPTION_FIELD: &str = "description";
    pub const LABELS_FIELD: &str = "labels";
    pub const IDENTIFIERS_FIELD: &str = "identifiers";
    pub const CONTENT_LEN_FIELD: &str = "content_len";
    pub const COUNT_FIELD: &str = "count";
    pub const VISIBILITY_FIELD: &str = "visibility";
    pub const CREATED_AT_FIELD: &str = "created_at";
    pub const LAST_MODIFIED_FIELD: &str = "last_modified";
    pub const AUTHORS_FIELD: &str = "authors";
    pub const LOCKED_FIELD: &str = "locked";
    pub const LICENSE_FIELD: &str = "license";
    pub const HASHES_FIELD: &str = "hashes";
    pub const LOCATION_FIELD: &str = "location";
    pub const TAGS_FIELD: &str = "tags";
    pub const EXPIRES_AT_FIELD: &str = "expires_at";
    pub const FIRST_NAME_FIELD: &str = "first_name";
    pub const LAST_NAME_FIELD: &str = "last_name";
    pub const EMAIL_FIELD: &str = "email";
    pub const GLOBAL_ADMIN_FIELD: &str = "global_admin";
    pub const TAG_FIELD: &str = "tag";
    pub const COMPONENT_TYPE_FIELD: &str = "component_type";
    pub const ENDPOINTS_FIELD: &str = "endpoints";
    pub const PUBLIC: &str = "public";
    pub const LICENSE_TERMS: &str = "terms";
}

// Milli index internal field ids
pub const FIELDS: &[Field] = &[
    Field {
        name: field_names::ID_FIELD,
        index: 0,
    }, // 0 UUID - This is the primary key             | ALL
    Field {
        name: field_names::VARIANT_FIELD,
        index: 1,
    }, // 1 Int - NodeVariant                          | ALL
    Field {
        name: field_names::DELETED_FIELD,
        index: 2,
    }, // 2 bool - Flags a node as deleted            | All
    Field {
        name: field_names::NAME_FIELD,
        index: 3,
    }, // 3 String - Name of the resource              | ALL
    Field {
        name: field_names::DESCRIPTION_FIELD,
        index: 4,
    }, // 4 String - Description of the resource       | ALL
    Field {
        name: field_names::LABELS_FIELD,
        index: 5,
    }, // 5 Value - Labels of the resource             | Resource
    Field {
        name: field_names::IDENTIFIERS_FIELD,
        index: 6,
    }, // 6 Value - List of external identifiers       | Resource
    Field {
        name: field_names::CONTENT_LEN_FIELD,
        index: 7,
    }, // 7 Int - Length of the content                | Resource
    Field {
        name: field_names::COUNT_FIELD,
        index: 8,
    }, // 8 Int - Count of the resource                | Resource
    Field {
        name: field_names::VISIBILITY_FIELD,
        index: 9,
    }, // 9 Int - Visibility of the resource           | Resource
    Field {
        name: field_names::CREATED_AT_FIELD,
        index: 10,
    }, // 10 Int - Creation time of the resource        | ALL
    Field {
        name: field_names::LAST_MODIFIED_FIELD,
        index: 11,
    }, // 11 Int - Last update time of the resource    | ALL
    Field {
        name: field_names::AUTHORS_FIELD,
        index: 12,
    }, // 12 Value - List of authors of the resource   | Resource
    Field {
        name: field_names::LOCKED_FIELD,
        index: 13,
    }, // 13 Bool - Is the resource read_only          | Resource
    Field {
        name: field_names::LICENSE_FIELD,
        index: 14,
    }, // 14 String - License of the resource          | Resource
    Field {
        name: field_names::HASHES_FIELD,
        index: 15,
    }, // 15 Value - Hashes of the resource            | Resource
    Field {
        name: field_names::LOCATION_FIELD,
        index: 16,
    }, // 16 Value - Location of the resource          | Resource
    Field {
        name: field_names::TAGS_FIELD,
        index: 17,
    }, // 17 Value - Tags of a realm                   | Realm
    Field {
        name: field_names::EXPIRES_AT_FIELD,
        index: 18,
    }, // 18 Int - Expiration time of the resource     | Token
    Field {
        name: field_names::FIRST_NAME_FIELD,
        index: 19,
    }, // 19 String - First name of the user           | User
    Field {
        name: field_names::LAST_NAME_FIELD,
        index: 20,
    }, // 20 String - Last name of the user            | User
    Field {
        name: field_names::EMAIL_FIELD,
        index: 21,
    }, // 21 String - Email of the user                | User
    Field {
        name: field_names::GLOBAL_ADMIN_FIELD,
        index: 22,
    }, // 22 Bool - Is the user a global admin         | User
    // TODO: Remove and merge into TAGS_FIELD
    Field {
        name: field_names::TAG_FIELD,
        index: 23,
    }, // 23 String - Tag or Title of a resource       | Realm / Resource
    Field {
        name: field_names::COMPONENT_TYPE_FIELD,
        index: 24,
    }, // 24 Int - Component variant                   | Component
    Field {
        name: field_names::ENDPOINTS_FIELD,
        index: 25,
    }, // 25 String - Endpoint variant of a component  | Component
    Field {
        name: field_names::PUBLIC,
        index: 26,
    }, // 26 String - Visiblity of component            | Component
    Field {
        name: field_names::LICENSE_TERMS,
        index: 27,
    }, // 27 String - Visiblity of component            | Component
];

pub mod relation_types {
    pub const HAS_PART: u32 = 0u32;
    pub const OWNS_PROJECT: u32 = 1u32;
    pub const PERMISSION_NONE: u32 = 2u32;
    pub const PERMISSION_READ: u32 = 3u32;
    pub const PERMISSION_APPEND: u32 = 4u32;
    pub const PERMISSION_WRITE: u32 = 5u32;
    pub const PERMISSION_ADMIN: u32 = 6u32;
    pub const SHARES_PERMISSION: u32 = 7u32;
    pub const OWNED_BY_USER: u32 = 8u32;
    pub const GROUP_PART_OF_REALM: u32 = 9u32;
    pub const GROUP_ADMINISTRATES_REALM: u32 = 10u32;
    pub const REALM_USES_COMPONENT: u32 = 11u32;
    pub const PROJECT_PART_OF_REALM: u32 = 12u32;
    pub const DEFAULT: u32 = 13u32;
}

pub fn const_relations() -> [RelationInfo; 14] {
    [
        // Resource only
        // Target can only have one origin
        RelationInfo {
            idx: relation_types::HAS_PART,
            forward_type: "HasPart".to_string(),
            backward_type: "PartOf".to_string(),
            internal: false,
        },
        // Group -> Project only
        RelationInfo {
            idx: relation_types::OWNS_PROJECT,
            forward_type: "OwnsProject".to_string(),
            backward_type: "ProjectOwnedBy".to_string(),
            internal: false,
        },
        //  User / Group / Token / ServiceAccount -> Resource only
        RelationInfo {
            idx: relation_types::PERMISSION_NONE,
            forward_type: "PermissionNone".to_string(),
            backward_type: "PermissionNone".to_string(),
            internal: true, // -> Displayed by resource request
        },
        RelationInfo {
            idx: 3,
            forward_type: "PermissionRead".to_string(),
            backward_type: "PermissionRead".to_string(),
            internal: true,
        },
        RelationInfo {
            idx: 4,
            forward_type: "PermissionAppend".to_string(),
            backward_type: "PermissionAppend".to_string(),
            internal: true,
        },
        RelationInfo {
            idx: 5,
            forward_type: "PermissionWrite".to_string(),
            backward_type: "PermissionWrite".to_string(),
            internal: true,
        },
        RelationInfo {
            idx: 6,
            forward_type: "PermissionAdmin".to_string(),
            backward_type: "PermissionAdmin".to_string(),
            internal: true,
        },
        // Group -> Group only
        RelationInfo {
            idx: 7,
            forward_type: "SharesPermissionTo".to_string(),
            backward_type: "PermissionSharedFrom".to_string(),
            internal: true,
        },
        // Token -> User only
        RelationInfo {
            idx: 8,
            forward_type: "OwnedByUser".to_string(),
            backward_type: "UserOwnsToken".to_string(),
            internal: true,
        },
        // Group -> Realm
        RelationInfo {
            idx: 9,
            forward_type: "GroupPartOfRealm".to_string(),
            backward_type: "RealmHasGroup".to_string(),
            internal: true,
        },
        // Mutually exclusive with GroupPartOfRealm
        // Can only have a connection to one realm
        // Group -> Realm
        RelationInfo {
            idx: 10,
            forward_type: "GroupAdministratesRealm".to_string(),
            backward_type: "RealmAdministratedBy".to_string(),
            internal: true,
        },
        // Realm -> Component
        RelationInfo {
            idx: 11,
            forward_type: "RealmUsesComponents".to_string(),
            backward_type: "ComponentUsedByRealm".to_string(),
            internal: true,
        },
        // Realm -> Project
        RelationInfo {
            idx: 12,
            forward_type: "ProjectPartOfRealm".to_string(),
            backward_type: "RealmHasProject".to_string(),
            internal: true,
        },
        // Default relation
        RelationInfo {
            idx: 13,
            forward_type: "DefaultOf".to_string(),
            backward_type: "HasDefault".to_string(),
            internal: true,
        },
    ]
}
