/// Compute the S3 common prefix for `key` under the given `prefix`/`delimiter`,
/// or `None` when the key has no delimiter past the prefix (so it is listed as
/// an individual entry). Shared by the delimiter-grouping list operations.
pub fn common_prefix_of(
    key: &str,
    prefix: Option<&str>,
    delimiter: Option<&str>,
) -> Option<String> {
    let delimiter = delimiter.filter(|delimiter| !delimiter.is_empty())?;
    let prefix_len = prefix.map_or(0, str::len);
    let relative_match = key.get(prefix_len..)?.find(delimiter)?;
    Some(key[..prefix_len + relative_match + delimiter.len()].to_string())
}
