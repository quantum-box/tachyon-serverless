//! Function drive: a per-environment read-only ext4 image holding `/app`.
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
    fn sparse_image_has_requested_length() {
        let dir = tempfile::tempdir().unwrap();
        let img = dir.path().join("function.ext4");
        create_sparse_image(&img, 9 * MIB).unwrap();
        assert_eq!(std::fs::metadata(&img).unwrap().len(), 9 * MIB);
    }
}
