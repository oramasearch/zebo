use std::{fmt::Display, path::PathBuf};

#[derive(Debug)]
pub enum ZeboError {
    UnsupportedVersion {
        version: u8,
        wanted: u8,
    },
    CannotCreateBaseDir {
        inner_error: std::io::Error,
        base_dir: PathBuf,
    },
    BaseDirIsNotDir {
        base_dir: PathBuf,
    },
    OpenPageFileError {
        inner_error: std::io::Error,
        page_file_path: PathBuf,
    },
    NotEnoughReservedSpace {
        wanted: usize,
        reserved: u32,
    },
    WrongReservedSpace {
        wanted: usize,
        reserved: u32,
    },
    NotEnoughSpace {
        limit: u32,
        new_allocation_requested: u32,
    },
    OperationError(std::io::Error),
    UnexpectedPageId,
}
impl Display for ZeboError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ZeboError::UnsupportedVersion { version, wanted } => {
                write!(f, "Unsupported version: {version}. Wanted: {wanted}")
            }
            ZeboError::CannotCreateBaseDir {
                inner_error,
                base_dir,
            } => {
                write!(
                    f,
                    "Cannot create base dir: {base_dir:?}. Error: {inner_error}"
                )
            }
            ZeboError::BaseDirIsNotDir { base_dir } => {
                write!(f, "Base dir is not a directory: {base_dir:?}")
            }
            ZeboError::OpenPageFileError {
                inner_error,
                page_file_path,
            } => {
                write!(
                    f,
                    "Cannot open page file: {page_file_path:?}. Error: {inner_error}"
                )
            }
            ZeboError::NotEnoughReservedSpace { wanted, reserved } => {
                write!(
                    f,
                    "Not enough reserved space. Wanted: {wanted}. Reserved: {reserved}"
                )
            }
            ZeboError::NotEnoughSpace {
                limit,
                new_allocation_requested,
            } => write!(
                f,
                "Not enough space. Limit: {limit}. New allocation requested: {new_allocation_requested}"
            ),
            ZeboError::WrongReservedSpace { reserved, wanted } => write!(
                f,
                "Reserved {reserved} space but given {wanted}. Disalignament are not permitted."
            ),

            ZeboError::OperationError(error) => write!(f, "Operation error: {error}"),
            ZeboError::UnexpectedPageId => write!(f, "Unexpected page ID"),
        }
    }
}
impl std::error::Error for ZeboError {}

pub type Result<T> = std::result::Result<T, ZeboError>;
