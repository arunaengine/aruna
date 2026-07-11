use std::io::{self, ErrorKind};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(crate) const MAX_CONTROL_PLANE_FRAME: usize = 128 * 1024 * 1024;

pub(crate) fn checked_frame_len(len: u32, max_len: usize) -> io::Result<usize> {
    let len = len as usize;
    if len == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            "refusing to allocate a zero-length frame",
        ));
    }
    if len > max_len {
        return Err(io::Error::new(
            ErrorKind::InvalidData,
            format!("frame length {len} exceeds maximum {max_len}"),
        ));
    }
    Ok(len)
}

pub(crate) fn checked_send_len(len: usize, max_len: usize) -> io::Result<u32> {
    if len == 0 {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            "refusing to send a zero-length frame",
        ));
    }
    if len > max_len {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("frame length {len} exceeds maximum {max_len}"),
        ));
    }
    Ok(len as u32)
}

pub(crate) async fn write_frame<W>(writer: &mut W, payload: &[u8], max_len: usize) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len = checked_send_len(payload.len(), max_len)?;
    writer.write_u32(len).await?;
    writer.write_all(payload).await?;
    writer.flush().await?;
    Ok(())
}

pub(crate) async fn read_frame<R>(reader: &mut R, max_len: usize) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let len = checked_frame_len(reader.read_u32().await?, max_len)?;
    let mut buf = vec![0; len];
    reader.read_exact(&mut buf).await?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_CONTROL_PLANE_FRAME, checked_frame_len, checked_send_len, read_frame, write_frame,
    };
    use std::io::ErrorKind;
    use tokio::io::AsyncWriteExt;

    const TEST_LIMIT: usize = 64;

    #[tokio::test]
    async fn round_trips_payload_at_limit() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        let payload = vec![7u8; TEST_LIMIT];
        write_frame(&mut writer, &payload, TEST_LIMIT)
            .await
            .unwrap();
        let received = read_frame(&mut reader, TEST_LIMIT).await.unwrap();
        assert_eq!(received, payload);
    }

    #[tokio::test]
    async fn rejects_length_one_over_limit_before_allocation() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        writer.write_u32(TEST_LIMIT as u32 + 1).await.unwrap();
        writer.flush().await.unwrap();
        let err = read_frame(&mut reader, TEST_LIMIT).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn rejects_u32_max_header_without_allocating() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        writer.write_u32(u32::MAX).await.unwrap();
        writer.flush().await.unwrap();
        let err = read_frame(&mut reader, TEST_LIMIT).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn rejects_zero_length_header() {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        writer.write_u32(0).await.unwrap();
        writer.flush().await.unwrap();
        let err = read_frame(&mut reader, TEST_LIMIT).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn refuses_to_send_oversized_payload() {
        let (mut writer, _reader) = tokio::io::duplex(4096);
        let payload = vec![0u8; TEST_LIMIT + 1];
        let err = write_frame(&mut writer, &payload, TEST_LIMIT)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }

    #[test]
    fn checked_frame_len_boundaries() {
        assert_eq!(
            checked_frame_len(TEST_LIMIT as u32, TEST_LIMIT).unwrap(),
            TEST_LIMIT
        );
        assert!(checked_frame_len(0, TEST_LIMIT).is_err());
        assert!(checked_frame_len(TEST_LIMIT as u32 + 1, TEST_LIMIT).is_err());
        assert!(checked_frame_len(u32::MAX, MAX_CONTROL_PLANE_FRAME).is_err());
    }

    #[test]
    fn checked_send_len_rejects_over_limit() {
        assert_eq!(
            checked_send_len(TEST_LIMIT, TEST_LIMIT).unwrap(),
            TEST_LIMIT as u32
        );
        assert!(checked_send_len(TEST_LIMIT + 1, TEST_LIMIT).is_err());
    }

    #[tokio::test]
    async fn rejects_empty_send() {
        let (mut writer, _reader) = tokio::io::duplex(4096);
        let err = write_frame(&mut writer, &[], TEST_LIMIT).await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::InvalidInput);
    }
}
