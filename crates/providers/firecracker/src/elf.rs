//! Minimal ELF64 inspection used to validate artifacts before they are put
//! on a function drive.
//!
//! Only the ELF header and the program header table are read; the rest of
//! the file is never touched, so validating a 200 MiB artifact is cheap.

use std::io::{Read, Seek, SeekFrom};

use tachyon_serverless_domain::Architecture;

pub const ELF_MAGIC: [u8; 4] = [0x7f, b'E', b'L', b'F'];
pub const ELFCLASS64: u8 = 2;
pub const ELFDATA2LSB: u8 = 1;
pub const ET_EXEC: u16 = 2;
pub const ET_DYN: u16 = 3;
pub const EM_X86_64: u16 = 0x3E;
pub const EM_AARCH64: u16 = 0xB7;
pub const PT_INTERP: u32 = 3;
pub const PT_DYNAMIC: u32 = 2;

pub const ELF64_HEADER_LEN: usize = 64;
pub const ELF64_PHDR_LEN: usize = 56;
/// Upper bound on the program header table we are willing to read.
const MAX_PHDR_TABLE_BYTES: u64 = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ElfInfo {
    pub machine: u16,
    pub elf_type: u16,
    /// `PT_INTERP` present: the binary needs a dynamic loader.
    pub has_interp: bool,
    /// `PT_DYNAMIC` present (static-pie binaries have it without `PT_INTERP`).
    pub has_dynamic: bool,
}

impl ElfInfo {
    pub fn architecture(&self) -> Option<Architecture> {
        architecture_of(self.machine)
    }
    /// True when the binary can run without a dynamic loader.
    pub fn is_static(&self) -> bool {
        !self.has_interp
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ElfError {
    #[error("file too short for an ELF64 header ({0} bytes)")]
    TooShort(usize),
    #[error("not an ELF file (bad magic)")]
    BadMagic,
    #[error("not a 64-bit ELF (EI_CLASS={0})")]
    NotElf64(u8),
    #[error("not little-endian (EI_DATA={0})")]
    NotLittleEndian(u8),
    #[error("not an executable (e_type={0:#x}; expected ET_EXEC or ET_DYN)")]
    NotExecutable(u16),
    #[error("program header table is invalid: {0}")]
    ProgramHeaders(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

pub fn machine_for(arch: Architecture) -> u16 {
    match arch {
        Architecture::X86_64 => EM_X86_64,
        Architecture::Aarch64 => EM_AARCH64,
    }
}

pub fn architecture_of(machine: u16) -> Option<Architecture> {
    match machine {
        EM_X86_64 => Some(Architecture::X86_64),
        EM_AARCH64 => Some(Architecture::Aarch64),
        _ => None,
    }
}

/// Inspect an in-memory ELF image.
pub fn inspect_elf_bytes(bytes: &[u8]) -> Result<ElfInfo, ElfError> {
    inspect_elf(&mut std::io::Cursor::new(bytes))
}

/// Inspect an ELF image through a seekable reader (header + program headers only).
pub fn inspect_elf<R: Read + Seek>(r: &mut R) -> Result<ElfInfo, ElfError> {
    let mut hdr = [0u8; ELF64_HEADER_LEN];
    let mut got = 0;
    while got < ELF64_HEADER_LEN {
        let n = r.read(&mut hdr[got..])?;
        if n == 0 {
            break;
        }
        got += n;
    }
    if got < ELF64_HEADER_LEN {
        return Err(ElfError::TooShort(got));
    }
    if hdr[..4] != ELF_MAGIC {
        return Err(ElfError::BadMagic);
    }
    if hdr[4] != ELFCLASS64 {
        return Err(ElfError::NotElf64(hdr[4]));
    }
    if hdr[5] != ELFDATA2LSB {
        return Err(ElfError::NotLittleEndian(hdr[5]));
    }
    let u16_at = |o: usize| u16::from_le_bytes([hdr[o], hdr[o + 1]]);
    let u64_at = |o: usize| {
        let mut b = [0u8; 8];
        b.copy_from_slice(&hdr[o..o + 8]);
        u64::from_le_bytes(b)
    };
    let elf_type = u16_at(16);
    let machine = u16_at(18);
    let phoff = u64_at(32);
    let phentsize = u16_at(54) as u64;
    let phnum = u16_at(56) as u64;
    if elf_type != ET_EXEC && elf_type != ET_DYN {
        return Err(ElfError::NotExecutable(elf_type));
    }

    let mut has_interp = false;
    let mut has_dynamic = false;
    if phnum > 0 {
        if phentsize < ELF64_PHDR_LEN as u64 {
            return Err(ElfError::ProgramHeaders(format!(
                "e_phentsize {phentsize} < {ELF64_PHDR_LEN}"
            )));
        }
        let table_len = phentsize.saturating_mul(phnum);
        if table_len > MAX_PHDR_TABLE_BYTES {
            return Err(ElfError::ProgramHeaders(format!(
                "program header table too large ({table_len} bytes)"
            )));
        }
        let mut table = vec![0u8; table_len as usize];
        r.seek(SeekFrom::Start(phoff))?;
        r.read_exact(&mut table).map_err(|e| {
            ElfError::ProgramHeaders(format!(
                "cannot read {table_len} bytes at e_phoff={phoff}: {e}"
            ))
        })?;
        for i in 0..phnum as usize {
            let off = i * phentsize as usize;
            let p_type =
                u32::from_le_bytes([table[off], table[off + 1], table[off + 2], table[off + 3]]);
            match p_type {
                PT_INTERP => has_interp = true,
                PT_DYNAMIC => has_dynamic = true,
                _ => {}
            }
        }
    }
    Ok(ElfInfo {
        machine,
        elf_type,
        has_interp,
        has_dynamic,
    })
}

/// Inspect an ELF file on disk.
pub fn inspect_elf_file(path: &std::path::Path) -> Result<ElfInfo, ElfError> {
    let mut f = std::fs::File::open(path)?;
    inspect_elf(&mut f)
}

/// Build a minimal ELF64 image (header + program headers) for tests.
#[cfg(test)]
pub(crate) fn craft_elf(machine: u16, elf_type: u16, phdr_types: &[u32]) -> Vec<u8> {
    let phnum = phdr_types.len();
    let mut v = vec![0u8; ELF64_HEADER_LEN + phnum * ELF64_PHDR_LEN];
    v[..4].copy_from_slice(&ELF_MAGIC);
    v[4] = ELFCLASS64;
    v[5] = ELFDATA2LSB;
    v[6] = 1; // EI_VERSION
    v[16..18].copy_from_slice(&elf_type.to_le_bytes());
    v[18..20].copy_from_slice(&machine.to_le_bytes());
    v[20..24].copy_from_slice(&1u32.to_le_bytes()); // e_version
    v[32..40].copy_from_slice(&(ELF64_HEADER_LEN as u64).to_le_bytes()); // e_phoff
    v[52..54].copy_from_slice(&(ELF64_HEADER_LEN as u16).to_le_bytes());
    v[54..56].copy_from_slice(&(ELF64_PHDR_LEN as u16).to_le_bytes());
    v[56..58].copy_from_slice(&(phnum as u16).to_le_bytes());
    for (i, t) in phdr_types.iter().enumerate() {
        let off = ELF64_HEADER_LEN + i * ELF64_PHDR_LEN;
        v[off..off + 4].copy_from_slice(&t.to_le_bytes());
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    const PT_LOAD: u32 = 1;

    #[test]
    fn static_x86_64_exec() {
        let img = craft_elf(EM_X86_64, ET_EXEC, &[PT_LOAD, PT_LOAD]);
        let info = inspect_elf_bytes(&img).unwrap();
        assert_eq!(info.architecture(), Some(Architecture::X86_64));
        assert!(info.is_static());
        assert!(!info.has_dynamic);
    }

    #[test]
    fn static_pie_aarch64_is_static() {
        // Rust musl targets emit static-pie: ET_DYN + PT_DYNAMIC, no PT_INTERP.
        let img = craft_elf(EM_AARCH64, ET_DYN, &[PT_LOAD, PT_DYNAMIC]);
        let info = inspect_elf_bytes(&img).unwrap();
        assert_eq!(info.architecture(), Some(Architecture::Aarch64));
        assert!(info.is_static());
        assert!(info.has_dynamic);
    }

    #[test]
    fn dynamically_linked_detected_via_pt_interp() {
        let img = craft_elf(EM_X86_64, ET_DYN, &[PT_LOAD, PT_INTERP, PT_DYNAMIC]);
        let info = inspect_elf_bytes(&img).unwrap();
        assert!(!info.is_static());
    }

    #[test]
    fn rejects_non_elf_and_wrong_class() {
        assert!(matches!(
            inspect_elf_bytes(b"#!/bin/sh\n"),
            Err(ElfError::TooShort(_))
        ));
        let mut junk = vec![0u8; 64];
        junk[..4].copy_from_slice(b"MZ\0\0");
        assert!(matches!(inspect_elf_bytes(&junk), Err(ElfError::BadMagic)));
        let mut elf32 = craft_elf(EM_X86_64, ET_EXEC, &[]);
        elf32[4] = 1;
        assert!(matches!(
            inspect_elf_bytes(&elf32),
            Err(ElfError::NotElf64(1))
        ));
        let mut rel = craft_elf(EM_X86_64, 1, &[]);
        rel[16] = 1;
        assert!(matches!(
            inspect_elf_bytes(&rel),
            Err(ElfError::NotExecutable(1))
        ));
    }

    #[test]
    fn unknown_machine_maps_to_none() {
        let img = craft_elf(0x28, ET_EXEC, &[]); // EM_ARM (32-bit)
        let info = inspect_elf_bytes(&img).unwrap();
        assert_eq!(info.architecture(), None);
    }

    #[test]
    fn truncated_program_headers_are_an_error() {
        let mut img = craft_elf(EM_X86_64, ET_EXEC, &[PT_LOAD, PT_LOAD]);
        img.truncate(ELF64_HEADER_LEN + 10);
        assert!(matches!(
            inspect_elf_bytes(&img),
            Err(ElfError::ProgramHeaders(_))
        ));
    }
}
