use aruna_core::structs::checksum::{ChecksumAlgorithm, ExpectedChecksum};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use http::HeaderMap;
use s3s::dto::{
    ChecksumMode, ChecksumType, CompleteMultipartUploadOutput, CopyObjectResult, GetObjectOutput,
    HeadObjectOutput, PutObjectOutput, UploadPartOutput,
};
use s3s::{S3Error, S3Result, s3_error};
use std::collections::HashMap;

const CONTENT_MD5: &str = "content-md5";
const X_AMZ_CHECKSUM_ALGORITHM: &str = "x-amz-checksum-algorithm";
const X_AMZ_SDK_CHECKSUM_ALGORITHM: &str = "x-amz-sdk-checksum-algorithm";
const X_AMZ_CHECKSUM_CRC32: &str = "x-amz-checksum-crc32";
const X_AMZ_CHECKSUM_CRC32C: &str = "x-amz-checksum-crc32c";
const X_AMZ_CHECKSUM_CRC64NVME: &str = "x-amz-checksum-crc64nvme";
const X_AMZ_CHECKSUM_SHA1: &str = "x-amz-checksum-sha1";
const X_AMZ_CHECKSUM_SHA256: &str = "x-amz-checksum-sha256";
const X_AMZ_CHECKSUM_MODE: &str = "x-amz-checksum-mode";
const X_AMZ_CHECKSUM_TYPE: &str = "x-amz-checksum-type";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UploadChecksumRequest {
    pub expected: Vec<ExpectedChecksum>,
    pub response_algorithm: Option<ChecksumAlgorithm>,
    pub checksum_type: ChecksumType,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct EncodedChecksums {
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_crc64nvme: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub checksum_type: Option<ChecksumType>,
}

pub trait ApplyChecksums {
    fn apply_checksums(&mut self, checksums: EncodedChecksums);
}

impl ApplyChecksums for PutObjectOutput {
    fn apply_checksums(&mut self, checksums: EncodedChecksums) {
        self.checksum_crc32 = checksums.checksum_crc32;
        self.checksum_crc32c = checksums.checksum_crc32c;
        self.checksum_crc64nvme = checksums.checksum_crc64nvme;
        self.checksum_sha1 = checksums.checksum_sha1;
        self.checksum_sha256 = checksums.checksum_sha256;
        self.checksum_type = checksums.checksum_type;
    }
}

impl ApplyChecksums for HeadObjectOutput {
    fn apply_checksums(&mut self, checksums: EncodedChecksums) {
        self.checksum_crc32 = checksums.checksum_crc32;
        self.checksum_crc32c = checksums.checksum_crc32c;
        self.checksum_crc64nvme = checksums.checksum_crc64nvme;
        self.checksum_sha1 = checksums.checksum_sha1;
        self.checksum_sha256 = checksums.checksum_sha256;
        self.checksum_type = checksums.checksum_type;
    }
}

impl ApplyChecksums for GetObjectOutput {
    fn apply_checksums(&mut self, checksums: EncodedChecksums) {
        self.checksum_crc32 = checksums.checksum_crc32;
        self.checksum_crc32c = checksums.checksum_crc32c;
        self.checksum_crc64nvme = checksums.checksum_crc64nvme;
        self.checksum_sha1 = checksums.checksum_sha1;
        self.checksum_sha256 = checksums.checksum_sha256;
        self.checksum_type = checksums.checksum_type;
    }
}

impl ApplyChecksums for CompleteMultipartUploadOutput {
    fn apply_checksums(&mut self, checksums: EncodedChecksums) {
        self.checksum_crc32 = checksums.checksum_crc32;
        self.checksum_crc32c = checksums.checksum_crc32c;
        self.checksum_crc64nvme = checksums.checksum_crc64nvme;
        self.checksum_sha1 = checksums.checksum_sha1;
        self.checksum_sha256 = checksums.checksum_sha256;
        self.checksum_type = checksums.checksum_type;
    }
}

impl ApplyChecksums for UploadPartOutput {
    fn apply_checksums(&mut self, checksums: EncodedChecksums) {
        self.checksum_crc32 = checksums.checksum_crc32;
        self.checksum_crc32c = checksums.checksum_crc32c;
        self.checksum_crc64nvme = checksums.checksum_crc64nvme;
        self.checksum_sha1 = checksums.checksum_sha1;
        self.checksum_sha256 = checksums.checksum_sha256;
    }
}

impl ApplyChecksums for CopyObjectResult {
    fn apply_checksums(&mut self, checksums: EncodedChecksums) {
        self.checksum_crc32 = checksums.checksum_crc32;
        self.checksum_crc32c = checksums.checksum_crc32c;
        self.checksum_crc64nvme = checksums.checksum_crc64nvme;
        self.checksum_sha1 = checksums.checksum_sha1;
        self.checksum_sha256 = checksums.checksum_sha256;
        self.checksum_type = checksums.checksum_type;
    }
}

pub enum ChecksumSelection {
    Requested(Option<ChecksumAlgorithm>),
    AllStored,
}

pub fn parse_upload_checksum_request(headers: &HeaderMap) -> S3Result<UploadChecksumRequest> {
    let expected = parse_expected_checksums(headers)?;
    let declared = parse_declared_algorithm(headers)?;
    let checksum_type = parse_checksum_type(headers)?;

    if declared.is_some() && expected.is_empty() {
        return Err(s3_error!(
            InvalidRequest,
            "A declared checksum algorithm requires a matching checksum header"
        ));
    }

    if let Some(algorithm) = declared
        && !expected
            .iter()
            .any(|checksum| checksum.algorithm == algorithm)
    {
        return Err(s3_error!(
            BadDigest,
            "Declared checksum algorithm does not match provided checksum header"
        ));
    }

    Ok(UploadChecksumRequest {
        response_algorithm: declared
            .or_else(|| expected.first().map(|checksum| checksum.algorithm)),
        expected,
        checksum_type,
    })
}

pub fn checksum_mode_enabled(headers: &HeaderMap) -> bool {
    headers
        .get(X_AMZ_CHECKSUM_MODE)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case(ChecksumMode::ENABLED))
}

pub fn encode_checksums(
    hashes: &HashMap<String, Vec<u8>>,
    selection: ChecksumSelection,
    checksum_type: ChecksumType,
) -> EncodedChecksums {
    let mut encoded = EncodedChecksums {
        checksum_type: Some(checksum_type),
        ..Default::default()
    };

    match selection {
        ChecksumSelection::Requested(Some(algorithm)) => {
            set_algorithm(&mut encoded, hashes, algorithm);
        }
        ChecksumSelection::Requested(None) => {
            encoded.checksum_type = None;
        }
        ChecksumSelection::AllStored => {
            for algorithm in [
                ChecksumAlgorithm::Crc32,
                ChecksumAlgorithm::Crc32c,
                ChecksumAlgorithm::Crc64Nvme,
                ChecksumAlgorithm::Sha1,
                ChecksumAlgorithm::Sha256,
            ] {
                set_algorithm(&mut encoded, hashes, algorithm);
            }
        }
    }

    encoded
}

fn parse_expected_checksums(headers: &HeaderMap) -> S3Result<Vec<ExpectedChecksum>> {
    let mut expected = Vec::new();

    for (name, algorithm) in [
        (CONTENT_MD5, ChecksumAlgorithm::Md5),
        (X_AMZ_CHECKSUM_CRC32, ChecksumAlgorithm::Crc32),
        (X_AMZ_CHECKSUM_CRC32C, ChecksumAlgorithm::Crc32c),
        (X_AMZ_CHECKSUM_CRC64NVME, ChecksumAlgorithm::Crc64Nvme),
        (X_AMZ_CHECKSUM_SHA1, ChecksumAlgorithm::Sha1),
        (X_AMZ_CHECKSUM_SHA256, ChecksumAlgorithm::Sha256),
    ] {
        if let Some(value) = header_str(headers, name)? {
            expected.push(ExpectedChecksum {
                algorithm,
                digest: decode_digest(algorithm, value)?,
            });
        }
    }

    Ok(expected)
}

fn parse_declared_algorithm(headers: &HeaderMap) -> S3Result<Option<ChecksumAlgorithm>> {
    let sdk_algorithm = header_str(headers, X_AMZ_SDK_CHECKSUM_ALGORITHM)?;
    let algorithm = header_str(headers, X_AMZ_CHECKSUM_ALGORITHM)?;

    let Some(value) = sdk_algorithm.or(algorithm) else {
        return Ok(None);
    };

    let parsed = match value {
        "CRC32" => ChecksumAlgorithm::Crc32,
        "CRC32C" => ChecksumAlgorithm::Crc32c,
        "CRC64NVME" => ChecksumAlgorithm::Crc64Nvme,
        "SHA1" => ChecksumAlgorithm::Sha1,
        "SHA256" => ChecksumAlgorithm::Sha256,
        _ => return Err(s3_error!(InvalidRequest, "Unsupported checksum algorithm")),
    };

    Ok(Some(parsed))
}

fn parse_checksum_type(headers: &HeaderMap) -> S3Result<ChecksumType> {
    let Some(value) = header_str(headers, X_AMZ_CHECKSUM_TYPE)? else {
        return Ok(ChecksumType::from_static(ChecksumType::FULL_OBJECT));
    };

    match value {
        ChecksumType::FULL_OBJECT => Ok(ChecksumType::from_static(ChecksumType::FULL_OBJECT)),
        ChecksumType::COMPOSITE => Ok(ChecksumType::from_static(ChecksumType::COMPOSITE)),
        _ => Err(s3_error!(InvalidRequest, "Unsupported checksum type")),
    }
}

fn decode_digest(algorithm: ChecksumAlgorithm, value: &str) -> S3Result<Vec<u8>> {
    let decoded = STANDARD.decode(value).map_err(|_| {
        s3_error!(
            InvalidRequest,
            "The {} checksum you specified is not valid base64 encoded",
            algorithm.hash_key()
        )
    })?;

    if decoded.len() != algorithm.digest_len() {
        return Err(s3_error!(
            InvalidDigest,
            "The {} checksum you specified is not valid.",
            algorithm.hash_key()
        ));
    }

    Ok(decoded)
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> S3Result<Option<&'a str>> {
    headers
        .get(name)
        .map(|value| {
            value
                .to_str()
                .map_err(|_| s3_error!(InvalidRequest, "Invalid checksum header"))
        })
        .transpose()
}

fn set_algorithm(
    output: &mut EncodedChecksums,
    hashes: &HashMap<String, Vec<u8>>,
    algorithm: ChecksumAlgorithm,
) {
    let Some(encoded) = hashes
        .get(algorithm.hash_key())
        .map(|value| STANDARD.encode(value))
    else {
        return;
    };

    match algorithm {
        ChecksumAlgorithm::Crc32 => output.checksum_crc32 = Some(encoded),
        ChecksumAlgorithm::Crc32c => output.checksum_crc32c = Some(encoded),
        ChecksumAlgorithm::Crc64Nvme => output.checksum_crc64nvme = Some(encoded),
        ChecksumAlgorithm::Sha1 => output.checksum_sha1 = Some(encoded),
        ChecksumAlgorithm::Sha256 => output.checksum_sha256 = Some(encoded),
        ChecksumAlgorithm::Md5 => {}
    }
}

pub fn checksum_mismatch_error() -> S3Error {
    s3_error!(
        BadDigest,
        "The checksum you specified did not match what we received."
    )
}

#[cfg(test)]
mod tests {
    use super::{
        ApplyChecksums, CONTENT_MD5, ChecksumSelection, X_AMZ_CHECKSUM_CRC32, X_AMZ_CHECKSUM_MODE,
        X_AMZ_CHECKSUM_TYPE, X_AMZ_SDK_CHECKSUM_ALGORITHM, checksum_mode_enabled, encode_checksums,
        parse_upload_checksum_request,
    };
    use aruna_core::structs::checksum::{ChecksumAlgorithm, HASH_CRC32};
    use http::HeaderMap;
    use s3s::dto::{ChecksumType, PutObjectOutput};
    use std::collections::HashMap;

    #[test]
    fn parses_upload_checksums_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_MD5, "XrY7u+Ae7tCTyyK7j1rNww==".parse().unwrap());
        headers.insert(X_AMZ_CHECKSUM_CRC32, "y/Q5Jg==".parse().unwrap());
        headers.insert(X_AMZ_SDK_CHECKSUM_ALGORITHM, "CRC32".parse().unwrap());

        let request = parse_upload_checksum_request(&headers).unwrap();

        assert_eq!(request.expected.len(), 2);
        assert_eq!(request.response_algorithm, Some(ChecksumAlgorithm::Crc32));
        assert_eq!(request.checksum_type.as_str(), ChecksumType::FULL_OBJECT);
    }

    #[test]
    fn defaults_checksum_type_to_full_object() {
        let request = parse_upload_checksum_request(&HeaderMap::new()).unwrap();

        assert_eq!(request.checksum_type.as_str(), ChecksumType::FULL_OBJECT);
    }

    #[test]
    fn parses_explicit_checksum_type() {
        let mut headers = HeaderMap::new();
        headers.insert(X_AMZ_CHECKSUM_TYPE, "COMPOSITE".parse().unwrap());

        let request = parse_upload_checksum_request(&headers).unwrap();

        assert_eq!(request.checksum_type.as_str(), ChecksumType::COMPOSITE);
    }

    #[test]
    fn rejects_declared_algorithm_without_inline_checksum() {
        let mut headers = HeaderMap::new();
        headers.insert(X_AMZ_SDK_CHECKSUM_ALGORITHM, "CRC32".parse().unwrap());

        let err = parse_upload_checksum_request(&headers).unwrap_err();

        assert_eq!(err.code().as_str(), "InvalidRequest");
    }

    #[test]
    fn rejects_malformed_md5() {
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_MD5, "nope".parse().unwrap());

        let err = parse_upload_checksum_request(&headers).unwrap_err();

        assert_eq!(err.code().as_str(), "InvalidDigest");
    }

    #[test]
    fn enables_checksum_mode_from_headers() {
        let mut headers = HeaderMap::new();
        headers.insert(X_AMZ_CHECKSUM_MODE, "ENABLED".parse().unwrap());

        assert!(checksum_mode_enabled(&headers));
    }

    #[test]
    fn applies_encoded_checksums_via_trait() {
        let mut hashes = HashMap::new();
        hashes.insert(HASH_CRC32.to_string(), vec![0xcb, 0xf4, 0x39, 0x26]);

        let checksums = encode_checksums(
            &hashes,
            ChecksumSelection::Requested(Some(ChecksumAlgorithm::Crc32)),
            ChecksumType::from_static(ChecksumType::FULL_OBJECT),
        );

        let mut output = PutObjectOutput::default();
        output.apply_checksums(checksums);

        assert_eq!(output.checksum_crc32.as_deref(), Some("y/Q5Jg=="));
        assert_eq!(
            output.checksum_type.as_ref().map(|value| value.as_str()),
            Some(ChecksumType::FULL_OBJECT)
        );
    }
}
