//! Unit tests for generic right expansion, CREATE grants and the command and information
//! class rights matrix.
// Copyright (c) 2026 The Aruna Contributors
// SPDX-License-Identifier: MIT or Apache-2.0

use super::*;

const EVERYTHING: Rights = Rights {
    read: true,
    write: true,
    delete: true,
};
const READER: Rights = Rights {
    read: true,
    write: false,
    delete: false,
};

#[test]
fn generic_bits_expand() {
    assert_eq!(expand_generic(GENERIC_READ), READ_MAPPING);
    assert_eq!(expand_generic(GENERIC_WRITE), WRITE_MAPPING);
    assert_eq!(expand_generic(GENERIC_EXECUTE), EXECUTE_MAPPING);
    assert_eq!(expand_generic(GENERIC_ALL), ALL_ACCESS);
    assert_eq!(
        expand_generic(GENERIC_READ | DELETE | MAXIMUM_ALLOWED),
        READ_MAPPING | DELETE | MAXIMUM_ALLOWED
    );
}

#[test]
fn grant_follows_rights() {
    let reader = allowed_mask(READER, false);
    assert_eq!(grant(GENERIC_READ, reader), Ok(READ_MAPPING));
    assert_eq!(grant(GENERIC_WRITE, reader), Err(status::ACCESS_DENIED));
    assert_eq!(
        grant(FILE_READ_DATA | DELETE, reader),
        Err(status::ACCESS_DENIED)
    );
    assert_eq!(grant(MAXIMUM_ALLOWED, reader), Ok(reader));
    assert_eq!(
        grant(MAXIMUM_ALLOWED | FILE_WRITE_DATA, reader),
        Err(status::ACCESS_DENIED),
        "explicit bits stay checked under MAXIMUM_ALLOWED"
    );
    let everything = allowed_mask(EVERYTHING, false);
    assert_eq!(
        grant(GENERIC_ALL, everything),
        Err(status::ACCESS_DENIED),
        "no WRITE_DAC"
    );
    assert_eq!(grant(0, reader), Ok(0));
    assert_eq!(
        grant(ACCESS_SYSTEM_SECURITY, everything),
        Err(status::PRIVILEGE_NOT_HELD)
    );
    assert_eq!(grant(0x0400_0000, everything), Err(status::ACCESS_DENIED));
}

#[test]
fn read_only_drops() {
    let mask = allowed_mask(EVERYTHING, true);
    assert_eq!(
        mask & (FILE_WRITE_DATA | FILE_APPEND_DATA | DELETE | FILE_WRITE_ATTRIBUTES),
        0
    );
    assert_ne!(mask & FILE_READ_DATA, 0);
    assert_eq!(mask & (WRITE_DAC | WRITE_OWNER | ACCESS_SYSTEM_SECURITY), 0);
    assert_eq!(allowed_mask(Rights::default(), false), SYNCHRONIZE);
}

#[test]
fn write_modes_ranges() {
    assert_eq!(
        check_write(FILE_WRITE_DATA, 10, 5),
        Ok((WriteMode::Overwrite, Some(10)))
    );
    assert_eq!(
        check_write(FILE_APPEND_DATA, 10, 5),
        Ok((WriteMode::Append, Some(10)))
    );
    assert_eq!(
        check_write(FILE_READ_DATA, 10, 5),
        Err(status::ACCESS_DENIED)
    );
    assert_eq!(
        check_write(FILE_APPEND_DATA, END_OFFSET, 5),
        Ok((WriteMode::Append, None))
    );
    assert_eq!(
        check_write(FILE_WRITE_DATA, u64::MAX - 1, 4),
        Err(status::INVALID_PARAMETER)
    );
    assert_eq!(
        check_write(FILE_WRITE_DATA, i64::MAX as u64, 1),
        Err(status::INVALID_PARAMETER)
    );
    assert_eq!(
        check_write(FILE_WRITE_DATA, 7, 0),
        Ok((WriteMode::Overwrite, Some(7)))
    );
    assert!(!append_allowed(3, 4), "interior write");
    assert!(append_allowed(4, 4), "at end of file");
    assert!(append_allowed(9, 4), "past end of file");
}

#[test]
fn command_rights_matrix() {
    assert_eq!(check_read(FILE_READ_DATA), Ok(()));
    assert_eq!(
        check_read(FILE_READ_ATTRIBUTES | FILE_WRITE_DATA),
        Err(status::ACCESS_DENIED)
    );
    assert_eq!(check_list(FILE_LIST_DIRECTORY), Ok(()));
    assert_eq!(check_list(FILE_READ_ATTRIBUTES), Err(status::ACCESS_DENIED));
    assert_eq!(check_flush(FILE_WRITE_DATA, false), Ok(()));
    assert_eq!(check_flush(FILE_APPEND_DATA, false), Ok(()));
    assert_eq!(
        check_flush(FILE_READ_DATA, false),
        Err(status::ACCESS_DENIED)
    );
    assert_eq!(check_flush(FILE_ADD_FILE, true), Ok(()));
    assert_eq!(check_flush(FILE_ADD_SUBDIRECTORY, true), Ok(()));
    assert_eq!(
        check_flush(FILE_LIST_DIRECTORY, true),
        Err(status::ACCESS_DENIED)
    );
}

#[test]
fn query_classes_matrix() {
    for file_class in [
        class::BASIC,
        class::ALL,
        class::NETWORK_OPEN,
        class::ATTRIBUTE_TAG,
    ] {
        assert_eq!(
            query_need(INFO_FILE, file_class, 0),
            Ok(FILE_READ_ATTRIBUTES)
        );
    }
    assert_eq!(query_need(INFO_FILE, class::FULL_EA, 0), Ok(FILE_READ_EA));
    for file_class in [class::EA, class::INTERNAL, class::STANDARD, class::STREAM] {
        assert_eq!(query_need(INFO_FILE, file_class, 0), Ok(0));
    }
    assert_eq!(
        query_need(INFO_FILE, class::NORMALIZED_NAME, 0),
        Err(status::NOT_SUPPORTED)
    );
    assert_eq!(
        query_need(INFO_FILE, 0x77, 0),
        Err(status::INVALID_INFO_CLASS)
    );
    assert_eq!(query_need(INFO_FILESYSTEM, volume::SIZE, 0), Ok(0));
    assert_eq!(
        query_need(INFO_FILESYSTEM, 0x7f, 0),
        Err(status::INVALID_INFO_CLASS)
    );
    assert_eq!(query_need(INFO_QUOTA, 0, 0), Err(status::NOT_SUPPORTED));
    assert_eq!(query_need(9, 0, 0), Err(status::INVALID_PARAMETER));
}

#[test]
fn security_masks_exact() {
    assert_eq!(
        query_need(INFO_SECURITY, 0, security::OWNER),
        Ok(READ_CONTROL)
    );
    assert_eq!(
        query_need(INFO_SECURITY, 0, security::GROUP | security::DACL),
        Ok(READ_CONTROL)
    );
    assert_eq!(
        query_need(INFO_SECURITY, 0, security::SACL),
        Ok(ACCESS_SYSTEM_SECURITY)
    );
    assert_eq!(
        query_need(INFO_SECURITY, 0, security::BACKUP),
        Ok(READ_CONTROL | ACCESS_SYSTEM_SECURITY)
    );
    let need = security_need(security::BACKUP);
    assert_eq!(require_all(READ_CONTROL, need), Err(status::ACCESS_DENIED));
    assert_eq!(
        require_all(ACCESS_SYSTEM_SECURITY, need),
        Err(status::ACCESS_DENIED)
    );
}

#[test]
fn set_classes_matrix() {
    assert_eq!(
        set_need(INFO_FILE, class::BASIC, 0),
        Ok(FILE_WRITE_ATTRIBUTES)
    );
    assert_eq!(
        set_need(INFO_FILE, class::ALLOCATION, 0),
        Ok(FILE_WRITE_DATA)
    );
    assert_eq!(
        set_need(INFO_FILE, class::END_OF_FILE, 0),
        Ok(FILE_WRITE_DATA)
    );
    for file_class in [class::RENAME, class::DISPOSITION, class::DISPOSITION_EX] {
        assert_eq!(set_need(INFO_FILE, file_class, 0), Ok(DELETE));
    }
    assert_eq!(
        set_need(INFO_FILE, class::LINK, 0),
        Err(status::NOT_SUPPORTED)
    );
    assert_eq!(
        set_need(INFO_FILE, 0x77, 0),
        Err(status::INVALID_INFO_CLASS)
    );
    assert_eq!(set_need(INFO_SECURITY, 0, security::DACL), Ok(WRITE_DAC));
    assert_eq!(
        set_need(INFO_FILESYSTEM, volume::VOLUME, 0),
        Err(status::NOT_SUPPORTED)
    );
}
