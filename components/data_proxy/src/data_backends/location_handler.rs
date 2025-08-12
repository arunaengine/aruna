use anyhow::bail;
use anyhow::Result;
use diesel_ulid::DieselUlid;
use nom::character::complete::u32;
use nom::combinator::eof;
use nom::multi::many_till;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    sequence::{preceded, terminated},
    IResult, Parser,
};
use rand::distributions::Alphanumeric;
use rand::thread_rng;
use rand::Rng;

use crate::CONFIG;
//backend_scheme="s3://{{PROJECT_NAME}}-{{RANDOM:10}}/{{COLLECTION_NAME}}/{{DATASET_NAME}}/{{RANDOM:10}}_{{OBJECT_NAME}}"
#[derive(Debug, Clone)]
pub enum Arguments {
    Project,
    ProjectId,
    Collection,
    CollectionId,
    Dataset,
    DatasetId,
    Object,
    ObjectId,
    EndpointId,
    Random(u32),
    Slash,
    Text(String),
}

impl Arguments {
    pub fn with_hierarchy(&self, hierarchy: &[Option<(DieselUlid, String)>; 4]) -> Option<String> {
        match self {
            Arguments::Project => hierarchy.first().cloned().flatten().map(|(_, b)| b),
            Arguments::ProjectId => hierarchy
                .first()
                .cloned()
                .flatten()
                .map(|(a, _)| a.to_string().to_ascii_lowercase()),
            Arguments::Collection => hierarchy.get(1).cloned().flatten().map(|(_, b)| b),
            Arguments::CollectionId => hierarchy
                .get(1)
                .cloned()
                .flatten()
                .map(|(a, _)| a.to_string().to_ascii_lowercase()),
            Arguments::Dataset => hierarchy.get(2).cloned().flatten().map(|(_, b)| b),
            Arguments::DatasetId => hierarchy
                .get(1)
                .cloned()
                .flatten()
                .map(|(a, _)| a.to_string().to_ascii_lowercase()),
            Arguments::Object => hierarchy.get(3).cloned().flatten().map(|(_, b)| b),
            Arguments::ObjectId => hierarchy
                .get(3)
                .cloned()
                .flatten()
                .map(|(a, _)| a.to_string().to_ascii_lowercase()),
            Arguments::EndpointId => {
                Some(CONFIG.proxy.endpoint_id.to_string().to_ascii_lowercase())
            }
            Arguments::Random(x) => Some(
                thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(*x as usize)
                    .map(char::from)
                    .collect::<String>()
                    .to_ascii_lowercase(),
            ),
            Arguments::Slash => Some("/".to_string()),
            Arguments::Text(x) => Some(x.clone()),
        }
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum SchemaVariant {
    S3,
    Filesystem,
}

#[derive(Debug, Clone)]
pub struct CompiledVariant {
    pub bucket_arguments: Vec<Arguments>,
    pub key_arguments: Vec<Arguments>,
    #[allow(dead_code)]
    pub schema: SchemaVariant,
}

impl CompiledVariant {
    pub fn new(scheme: &str) -> Result<Self> {
        match Self::compile(scheme) {
            Ok((_, x)) => Ok(x),
            Err(e) => {
                bail!("Error parsing scheme: {}", e)
            }
        }
    }

    pub fn to_names(&self, hierarchy: [Option<(DieselUlid, String)>; 4]) -> (String, String) {
        let mut bucket = String::new();
        for bucket_string in self
            .bucket_arguments
            .iter()
            .filter_map(|x| x.with_hierarchy(&hierarchy))
        {
            if &bucket_string == "/" {
                if bucket.is_empty() || bucket.ends_with('/') {
                    continue;
                } else {
                    bucket.push('/');
                }
            }
            bucket.push_str(&bucket_string);
        }

        let mut key = String::new();
        for part_string in self
            .key_arguments
            .iter()
            .filter_map(|x| x.with_hierarchy(&hierarchy))
        {
            if part_string == "/" {
                if key.ends_with('/') {
                    continue;
                } else {
                    key.push('/');
                }
            } else {
                key.push_str(&part_string);
            }
        }

        (bucket, key)
    }

    pub fn compile(input: &str) -> IResult<&str, Self> {
        alt((Self::compile_s3, Self::compile_filesystem)).parse(input)
    }

    pub fn compile_s3(scheme: &str) -> IResult<&str, Self> {
        let (input, _) = tag("s3://")(scheme)?;
        let (rest, args): (&str, Vec<Arguments>) = Self::compile_tags(input)?;

        let mut bucket = Vec::new();
        let mut key = Vec::new();
        let mut fill_bucket = true;
        for arg in args {
            if fill_bucket {
                if let Arguments::Slash = arg {
                    fill_bucket = false;
                    continue;
                }
                bucket.push(arg);
            } else {
                key.push(arg);
            }
        }

        if fill_bucket {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }
        Ok((
            rest,
            Self {
                bucket_arguments: bucket,
                key_arguments: key,
                schema: SchemaVariant::S3,
            },
        ))
    }

    pub fn compile_filesystem(scheme: &str) -> IResult<&str, Self> {
        let (input, _) = tag("file://")(scheme)?;
        let (rest, args): (&str, Vec<Arguments>) = Self::compile_tags(input)?;

        let mut bucket = Vec::new();
        let mut key = Vec::new();
        let mut fill_bucket = true;
        for arg in args {
            if fill_bucket {
                if let Arguments::Slash = arg {
                    fill_bucket = false;
                    continue;
                }
                bucket.push(arg);
            } else {
                key.push(arg);
            }
        }

        if fill_bucket {
            return Err(nom::Err::Failure(nom::error::Error::new(
                input,
                nom::error::ErrorKind::Verify,
            )));
        }

        Ok((
            rest,
            Self {
                bucket_arguments: bucket,
                key_arguments: key,
                schema: SchemaVariant::S3,
            },
        ))
    }

    pub fn compile_tag(input: &str) -> IResult<&str, Arguments> {
        alt((
            tag("{{PROJECT_NAME}}").map(|_| Arguments::Project),
            tag("{{PROJECT_ID}}").map(|_| Arguments::ProjectId),
            tag("{{COLLECTION_NAME}}").map(|_| Arguments::Collection),
            tag("{{COLLECTION_ID}}").map(|_| Arguments::CollectionId),
            tag("{{DATASET_NAME}}").map(|_| Arguments::Dataset),
            tag("{{DATASET_ID}}").map(|_| Arguments::DatasetId),
            tag("{{OBJECT_NAME}}").map(|_| Arguments::Object),
            tag("{{OBJECT_ID}}").map(|_| Arguments::ObjectId),
            tag("{{PROXY_ID}}").map(|_| Arguments::EndpointId),
            terminated(preceded(tag("{{RANDOM:"), u32), tag("}}")).map(Arguments::Random),
            tag("/").map(|_| Arguments::Slash),
            take_while(|c| c != '{' && c != '}' && c != '/')
                .map(|x: &str| Arguments::Text(x.to_string())),
        ))
        .parse(input)
    }

    pub fn compile_tags(input: &str) -> IResult<&str, Vec<Arguments>> {
        many_till(Self::compile_tag, eof)
            .parse(input)
            .map(|(x, (y, _))| (x, y))
    }
}
