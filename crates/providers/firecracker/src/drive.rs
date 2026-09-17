//! Per-environment drives.
//!
//! - Function drive: a read-only ext4 image holding `/app`.
//! - Scratch drive (PLT-4622): an empty, writable ext4 image of exactly the
//!   revision's `ephemeral_storage_mib`, mounted by the guest init at `/tmp`.
//!   It is the only guest-writable storage backed by the host disk (rootfs and
//!   function drive are attached read-only), so its size is the hard cap on
//!   the host disk a guest can consume. The image is preallocated with
//!   `fallocate` where the file system supports it, so the space is reserved
//!   when the environment is created — concurrent environments cannot
//!   overcommit the host disk and later fail inside the guest.
//!
//! Built with `mkfs.ext4 -q -F -d <stage> <image> <size>M` (e2fsprogs >= 1.43
//! for `-d`). The image file is pre-created as a sparse file of the final
//! size *and* the size is passed explicitly, which works with both mke2fs
//! variants (those that create missing image files and those that do not).

use std::ffi::OsString;
use std::path::Path;

pub const MIB: u64 = 1024 * 1024;
/// Free space added on top of the artifact for ext4 metadata / journal.
pub const FUNCTION_DRIVE_SLACK_BYTES: u64 = 8 * MIB;

/// Size of the function drive for an artifact: `artifact + 8 MiB`, rounded
/// up to a whole MiB.
pub fn function_drive_size_bytes(artifact_size: u64) -> u64 {
    (artifact_size + FUNCTION_DRIVE_SLACK_BYTES).div_ceil(MIB) * MIB
}

/// Arguments for `mkfs.ext4` (without the program name).
pub fn mkfs_args(stage_dir: &Path, image: &Path, size_bytes: u64) -> Vec<OsString> {
    debug_assert_eq!(size_bytes % MIB, 0, "size must be MiB aligned");
    vec![
        OsString::from("-q"),
        OsString::from("-F"),
        OsString::from("-d"),
        stage_dir.as_os_str().to_owned(),
        image.as_os_str().to_owned(),
        OsString::from(format!("{}M", size_bytes / MIB)),
    ]
}

/// Size of the scratch drive: exactly `ephemeral_storage_mib` MiB.
pub fn scratch_drive_size_bytes(ephemeral_storage_mib: u32) -> u64 {
    u64::from(ephemeral_storage_mib) * MIB
}

/// Arguments for `mkfs.ext4` (without the program name) for an empty scratch
/// file system: 4 KiB blocks (mke2fs would pick 1 KiB for small images), no
/// reserved blocks, no journal (the contents die with the environment), no
/// discard (which would punch holes into the preallocated image) and inode
/// tables initialised now instead of by a background thread in the guest.
pub fn scratch_mkfs_args(image: &Path, size_bytes: u64) -> Vec<OsString> {
    debug_assert_eq!(size_bytes % MIB, 0, "size must be MiB aligned");
    vec![
        OsString::from("-q"),
        OsString::from("-F"),
        OsString::from("-b"),
        OsString::from("4096"),
        OsString::from("-m"),
        OsString::from("0"),
        OsString::from("-O"),
        OsString::from("^has_journal"),
        OsString::from("-E"),
        OsString::from("nodiscard,lazy_itable_init=0"),
        OsString::from("-L"),
        OsString::from("tachyon-scratch"),
        image.as_os_str().to_owned(),
        OsString::from(format!("{}M", size_bytes / MIB)),
    ]
}

/// Create the image file with its final size and reserve its blocks on the
/// host. Returns whether the blocks are reserved: `true` after a successful
/// `fallocate` (Linux), `false` when the file system does not support it (or
/// on a non-Linux host) and the file is sparse instead. Running out of space
/// is an error, so an environment that cannot get its scratch space is never
/// started.
pub fn create_reserved_image(image: &Path, size_bytes: u64) -> std::io::Result<bool> {
    let f = std::fs::File::create(image)?;
    #[cfg(target_os = "linux")]
    {
        use std::os::fd::AsRawFd;
        let len = libc::off_t::try_from(size_bytes)
            .map_err(|_| std::io::Error::other("scratch drive size does not fit off_t"))?;
        // SAFETY: plain syscall on an open file descriptor we own.
        let rc = unsafe { libc::fallocate(f.as_raw_fd(), 0, 0, len) };
        if rc == 0 {
            return Ok(true);
        }
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() != Some(libc::EOPNOTSUPP) {
            return Err(err);
        }
    }
    f.set_len(size_bytes)?;
    Ok(false)
}

/// Create the (sparse) image file with its final size. The subsequent
/// `mkfs.ext4` call formats it in place.
pub fn create_sparse_image(image: &Path, size_bytes: u64) -> std::io::Result<()> {
    let f = std::fs::File::create(image)?;
    f.set_len(size_bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizing_rounds_up_to_mib_with_8mib_slack() {
        assert_eq!(function_drive_size_bytes(0), 8 * MIB);
        assert_eq!(function_drive_size_bytes(1), 9 * MIB);
        assert_eq!(function_drive_size_bytes(MIB), 9 * MIB);
        assert_eq!(function_drive_size_bytes(MIB + 1), 10 * MIB);
        assert_eq!(function_drive_size_bytes(3 * MIB - 1), 11 * MIB);
        assert_eq!(function_drive_size_bytes(100 * MIB), 108 * MIB);
    }

    #[test]
    fn mkfs_arguments_follow_protocol() {
        let args = mkfs_args(
            Path::new("/w/e/stage"),
            Path::new("/w/e/function.ext4"),
            9 * MIB,
        );
        let args: Vec<String> = args
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec!["-q", "-F", "-d", "/w/e/stage", "/w/e/function.ext4", "9M"]
        );
    }

    #[test]
    fn scratch_drive_is_exactly_the_requested_size() {
        assert_eq!(scratch_drive_size_bytes(32), 32 * MIB);
        assert_eq!(scratch_drive_size_bytes(256), 256 * MIB);
        let args: Vec<String> = scratch_mkfs_args(Path::new("/w/e/scratch.ext4"), 64 * MIB)
            .into_iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-q",
                "-F",
                "-b",
                "4096",
                "-m",
                "0",
                "-O",
                "^has_journal",
                "-E",
                "nodiscard,lazy_itable_init=0",
                "-L",
                "tachyon-scratch",
                "/w/e/scratch.ext4",
                "64M"
            ]
        );
    }

    #[test]
    fn reserved_image_has_requested_length() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("scratch.ext4");
        let reserved = create_reserved_image(&img, 4 * MIB).unwrap();
        let meta = std::fs::metadata(&img).unwrap();
        assert_eq!(meta.len(), 4 * MIB);
        if reserved {
            use std::os::unix::fs::MetadataExt;
            assert!(meta.blocks() * 512 >= 4 * MIB, "blocks are reserved");
        }
    }

    #[test]
    fn sparse_image_has_requested_length() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("function.ext4");
        create_sparse_image(&img, 9 * MIB).unwrap();
        assert_eq!(std::fs::metadata(&img).unwrap().len(), 9 * MIB);
    }
}
