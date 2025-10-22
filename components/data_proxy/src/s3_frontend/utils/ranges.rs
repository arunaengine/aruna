use anyhow::anyhow;
use anyhow::Result;
use pithos_lib::helpers::footer_parser::Footer;
use pithos_lib::helpers::structs::Range as ArunaRange;
use pithos_lib::pithos::structs::FileContextVariants;
use s3s::dto::Range as S3Range;
use s3s::dto::Range::{Int, Suffix};
use tracing::debug;

use crate::structs::ObjectLocation;

const RAW_CHUNK: u64 = 65536;
const ENCRYTPION_CHUNK: u64 = RAW_CHUNK + 28;

#[tracing::instrument(level = "trace", skip(input_range, content_length, footer))]
pub fn calculate_ranges(
    input_range: Option<S3Range>,
    content_length: u64,
    compressed_size: u64,
    footer: Option<Footer>,
    location: &ObjectLocation,
) -> Result<(Option<String>, Option<Vec<u64>>, u64, Option<ArunaRange>)> {
    let Some(range) = input_range else {
        return Ok((None, None, compressed_size, None));
    };
    let aruna_range = aruna_range_from_s3range(range, content_length);
    debug!(?aruna_range, "Calculated ArunaRange");
    if location.is_pithos() {
        let Some(footer) = footer else {
            return Err(anyhow!("Footer not found"));
        };
        let file = footer
            .table_of_contents
            .files
            .first()
            .ok_or_else(|| anyhow!("No files in footer"))?;
        let FileContextVariants::FileDecrypted(file) = file else {
            return Err(anyhow!("File not decrypted"));
        };
        let (mut a, b) = file.get_range_and_filter_by_range(pithos_lib::helpers::structs::Range {
            from: aruna_range.from,
            to: aruna_range.to,
        });

        if a.to > compressed_size {
            a.to = compressed_size;
        }

        return Ok((
            Some(format!("bytes={}-{}", a.from, a.to - 1)),
            Some(b),
            calculate_content_length_from_range(a),
            Some(aruna_range),
        ));
    }

    if location.is_compressed() {
        debug!("Compressed file");
        return Ok((
            None,
            Some(vec![aruna_range.from, aruna_range.to - aruna_range.from]),
            aruna_range.to - aruna_range.from,
            Some(aruna_range),
        ));
    }

    if location.get_encryption_key().is_some() {
        let start = aruna_range.from / RAW_CHUNK;
        let start_skip = aruna_range.from % RAW_CHUNK;
        let end = (aruna_range.to / RAW_CHUNK) + 1;

        return Ok((
            Some(format!(
                "bytes={}-{}",
                start * ENCRYTPION_CHUNK,
                end * ENCRYTPION_CHUNK
            )),
            Some(vec![start_skip, aruna_range.to - aruna_range.from]),
            aruna_range.to - aruna_range.from,
            Some(aruna_range),
        ));
    }

    Ok((
        Some(format!("bytes={}-{}", aruna_range.from, aruna_range.to)),
        None,
        aruna_range.to - aruna_range.from,
        Some(aruna_range),
    ))
}

#[tracing::instrument(level = "trace", skip(range))]
pub fn calculate_content_length_from_range(range: pithos_lib::helpers::structs::Range) -> u64 {
    range.to - range.from // Note: -1 bytes-ranges are inclusive
}

#[tracing::instrument(level = "trace", skip(range_string, content_length))]
pub fn aruna_range_from_s3range(range_string: S3Range, content_length: u64) -> ArunaRange {
    debug!(
        ?range_string,
        ?content_length,
        "Calculating ArunaRange from S3Range"
    );
    match range_string {
        Int { first, last } => match last {
            Some(val) => ArunaRange {
                from: first,
                to: if val > content_length {
                    content_length
                } else {
                    val + 1
                },
            },
            None => ArunaRange {
                from: first,
                to: content_length,
            },
        },
        Suffix { length } => ArunaRange {
            from: content_length - length,
            to: content_length,
        },
    }
}
