//! Maps forwarded multipart uploads: the coordinator of the key's range holds the upload, so
//! its creation, parts, completion and abort all go there and its answers come back as S3 output.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use s3s::dto::{
    CompleteMultipartUploadOutput, CompletedMultipartUpload, CreateMultipartUploadOutput, ETag,
    ListPartsOutput, Part, Timestamp, TimestampFormat, UploadPartOutput,
};
use s3s::{S3Response, S3Result, s3_error};

use super::ArunaS3Service;
use super::forward::{header, xml_field};

/// Escapes text for an XML element body.
fn escaped(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// The completion body the coordinator parses, built from the parts the caller listed.
pub(super) fn completion_body(upload: Option<&CompletedMultipartUpload>) -> String {
    let mut body = String::from("<CompleteMultipartUpload>");
    let parts = upload.and_then(|upload| upload.parts.as_ref());
    for part in parts.into_iter().flatten() {
        body.push_str("<Part>");
        if let Some(number) = part.part_number {
            body.push_str(&format!("<PartNumber>{number}</PartNumber>"));
        }
        if let Some(e_tag) = part.e_tag.as_ref() {
            body.push_str(&format!("<ETag>\"{}\"</ETag>", escaped(e_tag.value())));
        }
        let checksums = [
            ("ChecksumCRC32", part.checksum_crc32.as_ref()),
            ("ChecksumCRC32C", part.checksum_crc32c.as_ref()),
            ("ChecksumCRC64NVME", part.checksum_crc64nvme.as_ref()),
            ("ChecksumSHA1", part.checksum_sha1.as_ref()),
            ("ChecksumSHA256", part.checksum_sha256.as_ref()),
        ];
        for (tag, value) in checksums {
            if let Some(value) = value {
                body.push_str(&format!("<{tag}>{}</{tag}>", escaped(value)));
            }
        }
        body.push_str("</Part>");
    }
    body.push_str("</CompleteMultipartUpload>");
    body
}

/// The parts of a ListParts answer, in their order.
pub(super) fn listed_parts(body: &str) -> Vec<Part> {
    body.split("<Part>")
        .skip(1)
        .filter_map(|part| part.split("</Part>").next())
        .map(|part| Part {
            part_number: xml_field(part, "PartNumber").and_then(|value| value.parse().ok()),
            e_tag: e_tag(xml_field(part, "ETag")),
            size: xml_field(part, "Size").and_then(|value| value.parse().ok()),
            last_modified: xml_field(part, "LastModified")
                .and_then(|value| Timestamp::parse(TimestampFormat::DateTime, &value).ok()),
            checksum_crc32: xml_field(part, "ChecksumCRC32"),
            checksum_crc32c: xml_field(part, "ChecksumCRC32C"),
            checksum_crc64nvme: xml_field(part, "ChecksumCRC64NVME"),
            checksum_sha1: xml_field(part, "ChecksumSHA1"),
            checksum_sha256: xml_field(part, "ChecksumSHA256"),
            ..Default::default()
        })
        .collect()
}

fn e_tag(text: Option<String>) -> Option<ETag> {
    text.and_then(|value| ETag::parse_http_header(value.as_bytes()).ok())
}

impl ArunaS3Service {
    pub(super) async fn forwarded_create(
        response: reqwest::Response,
        bucket: String,
        key: String,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let body = response.text().await.unwrap_or_default();
        let upload_id = xml_field(&body, "UploadId")
            .ok_or_else(|| s3_error!(InternalError, "The coordinator named no upload id"))?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: Some(upload_id),
            ..Default::default()
        }))
    }

    pub(super) async fn forwarded_list(
        response: reqwest::Response,
        bucket: String,
        key: String,
    ) -> S3Result<S3Response<ListPartsOutput>> {
        let body = response.text().await.unwrap_or_default();
        let number = |tag: &str| xml_field(&body, tag).and_then(|value| value.parse().ok());
        Ok(S3Response::new(ListPartsOutput {
            bucket: Some(bucket),
            key: Some(key),
            upload_id: xml_field(&body, "UploadId"),
            is_truncated: xml_field(&body, "IsTruncated").map(|value| value == "true"),
            part_number_marker: number("PartNumberMarker"),
            next_part_number_marker: number("NextPartNumberMarker"),
            max_parts: number("MaxParts"),
            parts: Some(listed_parts(&body)),
            ..Default::default()
        }))
    }

    pub(super) fn forwarded_part(response: &reqwest::Response) -> S3Response<UploadPartOutput> {
        S3Response::new(UploadPartOutput {
            e_tag: e_tag(header(response, "etag")),
            checksum_crc32: header(response, "x-amz-checksum-crc32"),
            checksum_crc32c: header(response, "x-amz-checksum-crc32c"),
            checksum_crc64nvme: header(response, "x-amz-checksum-crc64nvme"),
            checksum_sha1: header(response, "x-amz-checksum-sha1"),
            checksum_sha256: header(response, "x-amz-checksum-sha256"),
            ..Default::default()
        })
    }

    /// A completion can fail after its status line, so an error body still counts as failure.
    pub(super) async fn forwarded_complete(
        response: reqwest::Response,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        let version_id = header(&response, "x-amz-version-id");
        let body = response.text().await.unwrap_or_default();
        if body.contains("<Error>") {
            let code = xml_field(&body, "Code").unwrap_or_default();
            let message = xml_field(&body, "Message").unwrap_or_default();
            return Err(s3_error!(
                InternalError,
                "The coordinator failed the completion: {code} {message}"
            ));
        }
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: xml_field(&body, "Bucket"),
            key: xml_field(&body, "Key"),
            location: xml_field(&body, "Location"),
            e_tag: e_tag(xml_field(&body, "ETag")),
            checksum_crc32: xml_field(&body, "ChecksumCRC32"),
            checksum_crc32c: xml_field(&body, "ChecksumCRC32C"),
            checksum_crc64nvme: xml_field(&body, "ChecksumCRC64NVME"),
            checksum_sha1: xml_field(&body, "ChecksumSHA1"),
            checksum_sha256: xml_field(&body, "ChecksumSHA256"),
            version_id,
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use s3s::dto::CompletedPart;

    #[test]
    fn completion_lists_parts() {
        let upload = CompletedMultipartUpload {
            parts: Some(vec![
                CompletedPart {
                    part_number: Some(1),
                    e_tag: Some(ETag::Strong("abc".to_string())),
                    checksum_sha256: Some("c2hh".to_string()),
                    ..Default::default()
                },
                CompletedPart {
                    part_number: Some(2),
                    e_tag: Some(ETag::Strong("d<e".to_string())),
                    ..Default::default()
                },
            ]),
        };
        let body = completion_body(Some(&upload));
        assert_eq!(
            body,
            "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>\"abc\"</ETag>\
             <ChecksumSHA256>c2hh</ChecksumSHA256></Part><Part><PartNumber>2</PartNumber>\
             <ETag>\"d&lt;e\"</ETag></Part></CompleteMultipartUpload>"
        );
        assert_eq!(
            completion_body(None),
            "<CompleteMultipartUpload></CompleteMultipartUpload>"
        );
    }

    #[test]
    fn listed_parts_parse() {
        let body = "<ListPartsResult><Bucket>b</Bucket><Part><PartNumber>1</PartNumber>\
                    <ETag>&quot;abc&quot;</ETag><Size>5</Size>\
                    <LastModified>2026-10-24T10:00:00.000Z</LastModified></Part>\
                    <Part><PartNumber>2</PartNumber><Size>7</Size></Part></ListPartsResult>";
        let parts = listed_parts(body);
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0].part_number, Some(1));
        assert_eq!(parts[0].size, Some(5));
        assert_eq!(parts[0].e_tag.as_ref().map(ETag::value), Some("abc"));
        assert!(parts[0].last_modified.is_some());
        assert_eq!(parts[1].part_number, Some(2));
    }
}
