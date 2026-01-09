use http::{HeaderMap, HeaderValue};
use s3s::{s3_error, S3Error};
use std::collections::HashMap;
use std::fmt::Formatter;

pub const CRC32_HEADER: &str = "x-amz-checksum-crc32";
pub const CRC32C_HEADER: &str = "x-amz-checksum-crc32c";
pub const CRC64NVME_HEADER: &str = "x-amz-checksum-crc64nvme";
pub const SHA1_HEADER: &str = "x-amz-checksum-sha1";
pub const SHA256_HEADER: &str = "x-amz-checksum-sha256";

#[derive(Clone, Debug)]
pub enum IntegrityChecksum {
    CRC32(Option<String>),
    CRC32C(Option<String>),
    CRC64NVME(Option<String>),
    SHA1(Option<String>), // For the sake of completeness. Not supported.
    SHA256(Option<String>),
}

impl std::fmt::Display for IntegrityChecksum {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IntegrityChecksum::CRC32(_) => write!(f, "crc32"),
            IntegrityChecksum::CRC32C(_) => write!(f, "crc32c"),
            IntegrityChecksum::CRC64NVME(_) => write!(f, "crc64nvme"),
            IntegrityChecksum::SHA1(_) => write!(f, "sha1"),
            IntegrityChecksum::SHA256(_) => write!(f, "sha256"),
        }
    }
}

impl TryFrom<&HeaderValue> for IntegrityChecksum {
    type Error = S3Error;

    #[tracing::instrument(level = "trace", skip(value))]
    fn try_from(value: &HeaderValue) -> Result<Self, Self::Error> {
        let header_value_str = value
            .to_str()
            .map_err(|_| s3_error!(InvalidArgument, "header contains invalid value"))?;
        match header_value_str {
            "CRC32" => Ok(IntegrityChecksum::CRC32(None)),
            "CRC32C" => Ok(IntegrityChecksum::CRC32C(None)),
            "CRC64NVME" => Ok(IntegrityChecksum::CRC64NVME(None)),
            "SHA256" => Ok(IntegrityChecksum::SHA256(None)),
            "SHA1" => Err(s3_error!(
                NotImplemented,
                "Sha1 checksum integrity validation is not supported"
            )),
            _ => Err(s3_error!(InvalidArgument, "invalid checksum algorithm")),
        }
    }
}

impl TryFrom<(&str, Option<&HeaderValue>)> for IntegrityChecksum {
    type Error = S3Error;

    fn try_from(value: (&str, Option<&HeaderValue>)) -> Result<Self, Self::Error> {
        let provided_checksum = match value.1 {
            None => None,
            Some(val) => Some(
                val.to_str()
                    .map_err(|_| s3_error!(InvalidArgument, "header contains invalid characters"))?
                    .to_string(),
            ),
        };

        match value.0 {
            "CRC32" => Ok(IntegrityChecksum::CRC32(provided_checksum)),
            "CRC32C" => Ok(IntegrityChecksum::CRC32C(provided_checksum)),
            "CRC64NVME" => Ok(IntegrityChecksum::CRC64NVME(provided_checksum)),
            "SHA256" => Ok(IntegrityChecksum::SHA256(provided_checksum)),
            "SHA1" => Err(s3_error!(
                NotImplemented,
                "Sha1 checksum integrity validation is not supported"
            )),
            _ => Err(s3_error!(InvalidArgument, "invalid checksum algorithm")),
        }
    }
}

impl IntegrityChecksum {
    /// Get the checksum header name for this algorithm
    pub fn checksum_header_name(&self) -> &str {
        match self {
            Self::CRC32(_) => CRC32_HEADER,
            Self::CRC32C(_) => CRC32C_HEADER,
            Self::CRC64NVME(_) => CRC64NVME_HEADER,
            Self::SHA1(_) => SHA1_HEADER,
            Self::SHA256(_) => SHA256_HEADER,
        }
    }

    pub fn set_validation_checksum(&mut self, checksum: String) {
        match self {
            IntegrityChecksum::CRC32(ref mut val) => *val = Some(checksum),
            IntegrityChecksum::CRC32C(ref mut val) => *val = Some(checksum),
            IntegrityChecksum::CRC64NVME(ref mut val) => *val = Some(checksum),
            IntegrityChecksum::SHA1(ref mut val) => *val = Some(checksum),
            IntegrityChecksum::SHA256(ref mut val) => *val = Some(checksum),
        }
    }
}

pub fn eval_required_checksum(
    headers: &HeaderMap<HeaderValue>,
) -> Result<Option<IntegrityChecksum>, S3Error> {
    // Check if "x-amz-sdk-checksum-algorithm" is present
    if let Some(header_value) = headers.get("x-amz-sdk-checksum-algorithm") {
        let mut req_checksum = IntegrityChecksum::try_from(header_value)?;

        // Enforce that the corresponding "x-amz-checksum-algorithm" or "x-amz-trailer" is present
        let checksum_header = req_checksum.checksum_header_name();
        if let Some(validation_checksum) = headers.get(checksum_header) {
            let checksum_str = header_value_to_str(validation_checksum)?.to_string();
            req_checksum.set_validation_checksum(checksum_str);
        } else if let Some(trailer) = headers.get("x-amz-trailer") {
            let trailer_header: Vec<&str> = header_value_to_str(trailer)?
                .split(',')
                .map(|t| t.trim())
                .collect();

            if !trailer_header.contains(&checksum_header) {
                return Err(s3_error!(
                    MissingSecurityHeader,
                    "Integrity validation is missing required header"
                ));
            }
        }

        return Ok(Some(req_checksum));
    }

    // Else check if any standalone "x-amz-checksum-algorithm" is present
    let checksum = detect_algorithm_from_headers(headers)?;
    Ok(checksum)
}

/// Detect algorithm from individual checksum headers
fn detect_algorithm_from_headers(
    headers: &HeaderMap<HeaderValue>,
) -> Result<Option<IntegrityChecksum>, S3Error> {
    let (algo, header) = if headers.contains_key(CRC32_HEADER) {
        ("CRC32", CRC32_HEADER)
    } else if headers.contains_key(CRC32C_HEADER) {
        ("CRC32C", CRC32C_HEADER)
    } else if headers.contains_key(CRC64NVME_HEADER) {
        ("CRC64NVME", CRC64NVME_HEADER)
    } else if headers.contains_key(SHA1_HEADER) {
        ("SHA1", SHA1_HEADER)
    } else if headers.contains_key(SHA256_HEADER) {
        ("SHA256", SHA256_HEADER)
    } else {
        return Ok(None);
    };

    Ok(Some(IntegrityChecksum::try_from((
        algo,
        headers.get(header),
    ))?))
}

#[derive(Clone, Debug, Default)]
pub struct ChecksumHandler {
    pub required_checksum: Option<IntegrityChecksum>,
    pub calculated_checksums: HashMap<String, String>,
}

impl ChecksumHandler {
    pub fn new(required_checksum: Option<IntegrityChecksum>) -> Self {
        ChecksumHandler {
            required_checksum,
            calculated_checksums: HashMap::new(),
        }
    }

    pub fn get_validation_checksum(&self) -> &Option<String> {
        if let Some(checksum) = &self.required_checksum {
            match checksum {
                IntegrityChecksum::CRC32(val)
                | IntegrityChecksum::CRC32C(val)
                | IntegrityChecksum::CRC64NVME(val)
                | IntegrityChecksum::SHA1(val)
                | IntegrityChecksum::SHA256(val) => val,
            }
        } else {
            &None
        }
    }

    pub fn add_calculated_checksum(&mut self, key: impl Into<String>, checksum: String) {
        self.calculated_checksums.insert(key.into(), checksum);
    }

    pub fn get_calculated_checksum(&self) -> Option<String> {
        if let Some(algo) = &self.required_checksum {
            self.calculated_checksums.get(&algo.to_string()).cloned()
        } else {
            None
        }
    }

    pub fn get_calculated_checksums(&self) -> &HashMap<String, String> {
        &self.calculated_checksums
    }

    pub fn get_checksum_by_key(&self, key: &str) -> Option<String> {
        self.calculated_checksums.get(key).cloned()
    }

    pub fn upsert_checksum(&mut self, key: &str, checksum: &str) -> Option<String> {
        self.calculated_checksums
            .insert(key.to_string(), checksum.to_string())
    }

    pub fn validate_checksum(&self) -> bool {
        if let Some(checksum) = &self.required_checksum {
            let calculated = self
                .calculated_checksums
                .get(&checksum.to_string())
                .cloned();
            return self.get_validation_checksum().eq(&calculated);
        }

        // If no checksum is required
        true
    }
}

pub fn header_value_to_str(value: &HeaderValue) -> Result<&str, S3Error> {
    value
        .to_str()
        .map_err(|_| s3_error!(InvalidArgument, "invalid header value"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http::{HeaderMap, HeaderValue};

    #[test]
    fn test_display_variants() {
        assert_eq!(IntegrityChecksum::CRC32(None).to_string(), "crc32");
        assert_eq!(IntegrityChecksum::CRC32C(None).to_string(), "crc32c");
        assert_eq!(IntegrityChecksum::CRC64NVME(None).to_string(), "crc64nvme");
        assert_eq!(IntegrityChecksum::SHA1(None).to_string(), "sha1");
        assert_eq!(IntegrityChecksum::SHA256(None).to_string(), "sha256");

        assert_eq!(
            IntegrityChecksum::CRC32(Some("lorem".into())).to_string(),
            "crc32"
        );
        assert_eq!(
            IntegrityChecksum::CRC32C(Some("lorem".into())).to_string(),
            "crc32c"
        );
        assert_eq!(
            IntegrityChecksum::CRC64NVME(Some("lorem".into())).to_string(),
            "crc64nvme"
        );
        assert_eq!(
            IntegrityChecksum::SHA1(Some("lorem".into())).to_string(),
            "sha1"
        );
        assert_eq!(
            IntegrityChecksum::SHA256(Some("lorem".into())).to_string(),
            "sha256"
        );
    }

    #[test]
    fn test_try_from_headervalue_ok_and_err() {
        let hv_crc32 = HeaderValue::from_static("CRC32");
        let res = IntegrityChecksum::try_from(&hv_crc32);
        assert!(res.is_ok());
        match res.unwrap() {
            IntegrityChecksum::CRC32(val) => assert!(val.is_none()),
            _ => panic!("Expected CRC32 variant"),
        }

        let hv_invalid = HeaderValue::from_static("INVALID");
        let res_invalid = IntegrityChecksum::try_from(&hv_invalid);
        assert!(res_invalid.is_err());

        // SHA1 is explicitly not implemented
        let hv_sha1 = HeaderValue::from_static("SHA1");
        let res_sha1 = IntegrityChecksum::try_from(&hv_sha1);
        assert!(res_sha1.is_err());
    }

    #[test]
    fn test_try_from_tuple_ok_and_err() {
        let v = ("CRC32", Some(&HeaderValue::from_static("abcd")));
        let res = IntegrityChecksum::try_from(v);
        assert!(res.is_ok());
        match res.unwrap() {
            IntegrityChecksum::CRC32(val) => assert_eq!(val, Some("abcd".to_string())),
            _ => panic!("Expected CRC32 variant"),
        }

        let v_none = ("CRC32", None);
        let res_none = IntegrityChecksum::try_from(v_none);
        assert!(res_none.is_ok());
        match res_none.unwrap() {
            IntegrityChecksum::CRC32(val) => assert!(val.is_none()),
            _ => panic!("Expected CRC32 variant"),
        }

        let v_invalid = ("NOT_AN_ALGO", None);
        assert!(IntegrityChecksum::try_from(v_invalid).is_err());
    }

    #[test]
    fn test_checksum_header_name_and_set_validation() {
        let mut ic = IntegrityChecksum::CRC32(None);
        assert_eq!(ic.checksum_header_name(), CRC32_HEADER);
        ic.set_validation_checksum("val1".to_string());
        match ic {
            IntegrityChecksum::CRC32(Some(v)) => assert_eq!(v, "val1"),
            _ => panic!("Expected CRC32(Some)"),
        }

        let mut ic2 = IntegrityChecksum::SHA256(None);
        assert_eq!(ic2.checksum_header_name(), SHA256_HEADER);
        ic2.set_validation_checksum("val2".to_string());
        match ic2 {
            IntegrityChecksum::SHA256(Some(v)) => assert_eq!(v, "val2"),
            _ => panic!("Expected SHA256(Some)"),
        }
    }

    #[test]
    fn test_eval_required_checksum_sdk_with_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-sdk-checksum-algorithm",
            HeaderValue::from_static("CRC32"),
        );
        headers.insert(CRC32_HEADER, HeaderValue::from_static("mycrc"));

        let res = eval_required_checksum(&headers).expect("should parse checksum");
        assert!(res.is_some());
        match res.unwrap() {
            IntegrityChecksum::CRC32(val) => assert_eq!(val, Some("mycrc".to_string())),
            _ => panic!("Expected CRC32"),
        }
    }

    #[test]
    fn test_eval_required_checksum_sdk_with_trailer() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-sdk-checksum-algorithm",
            HeaderValue::from_static("CRC32"),
        );
        headers.insert("x-amz-trailer", HeaderValue::from_static(CRC32_HEADER));

        let res = eval_required_checksum(&headers).expect("should parse checksum from trailer");
        assert!(res.is_some());
        match res.unwrap() {
            IntegrityChecksum::CRC32(val) => assert!(val.is_none()),
            _ => panic!("Expected CRC32"),
        }
    }

    #[test]
    fn test_eval_required_checksum_sdk_missing_trailer_or_header() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "x-amz-sdk-checksum-algorithm",
            HeaderValue::from_static("CRC32"),
        );
        headers.insert(
            "x-amz-trailer",
            HeaderValue::from_static("some-other-header"),
        );

        let res = eval_required_checksum(&headers);
        assert!(res.is_err());
    }

    #[test]
    fn test_detect_algorithm_from_headers_via_eval() {
        let mut headers = HeaderMap::new();
        headers.insert(CRC32_HEADER, HeaderValue::from_static("detected"));
        let res = eval_required_checksum(&headers).expect("should detect algorithm");
        assert!(res.is_some());
        match res.unwrap() {
            IntegrityChecksum::CRC32(val) => assert_eq!(val, Some("detected".to_string())),
            _ => panic!("Expected CRC32"),
        }
    }

    #[test]
    fn test_checksum_handler_operations() {
        // No required checksum
        let handler = ChecksumHandler::new(None);
        assert!(handler.get_validation_checksum().is_none());
        assert!(handler.get_calculated_checksum().is_none());
        assert!(handler.validate_checksum()); // no required checksum => true

        // With required checksum
        let required = IntegrityChecksum::CRC32(None);
        let mut handler = ChecksumHandler::new(Some(required));
        // set validation checksum inside the handler
        if let Some(ic) = &mut handler.required_checksum {
            ic.set_validation_checksum("expected".to_string());
        }
        assert_eq!(
            handler.get_validation_checksum(),
            &Some("expected".to_string())
        );

        // add calculated checksum and retrieve
        handler.add_calculated_checksum("crc32", "expected".to_string());
        assert_eq!(
            handler.get_calculated_checksum(),
            Some("expected".to_string())
        );
        assert_eq!(
            handler.get_checksum_by_key("crc32"),
            Some("expected".to_string())
        );

        // upsert returns previous value
        let prev = handler.upsert_checksum("crc32", "newval");
        assert_eq!(prev, Some("expected".to_string()));
        assert_eq!(
            handler.get_checksum_by_key("crc32"),
            Some("newval".to_string())
        );

        // validate true only if calculated matches validation
        // currently validation is "expected" and calculated is "newval" -> false
        assert!(!handler.validate_checksum());

        // set calculated to match
        handler.upsert_checksum("crc32", "expected");
        assert!(handler.validate_checksum());
    }

    #[test]
    fn test_header_value_to_str() {
        let hv = HeaderValue::from_static("somestring");
        let s = header_value_to_str(&hv).expect("should convert");
        assert_eq!(s, "somestring");

        let invalid_hv =
            HeaderValue::from_bytes(&[115, 195, 182, 109, 195, 169, 115, 116, 114, 105, 110, 103])
                .unwrap();
        let res = header_value_to_str(&invalid_hv);
        assert!(res.is_err());
    }
}
