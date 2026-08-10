use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};

use tokio::process::Command;

const HOMEBREW_APPLE_SILICON_BIN: &str = "/opt/homebrew/bin";
const HOMEBREW_INTEL_BIN: &str = "/usr/local/bin";
const SYSTEM_BIN_DIRECTORIES: [&str; 4] = ["/usr/bin", "/bin", "/usr/sbin", "/sbin"];

pub(crate) fn configure_local_worker_command(command: &mut Command, executable: &str) {
    let inherited_path = std::env::var_os("PATH");
    let home = std::env::var_os("HOME");
    command.env(
        "PATH",
        local_codex_path(executable, inherited_path.as_deref(), home.as_deref()),
    );
}

fn local_codex_path(
    executable: &str,
    inherited_path: Option<&OsStr>,
    home: Option<&OsStr>,
) -> OsString {
    let mut directories = Vec::new();

    if let Some(parent) = Path::new(executable)
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        push_unique(&mut directories, parent.to_path_buf());
    }
    push_unique(&mut directories, PathBuf::from(HOMEBREW_APPLE_SILICON_BIN));
    push_unique(&mut directories, PathBuf::from(HOMEBREW_INTEL_BIN));
    if let Some(home) = home {
        push_unique(&mut directories, Path::new(home).join(".local/bin"));
    }
    if let Some(inherited_path) = inherited_path {
        for directory in std::env::split_paths(inherited_path) {
            push_unique(&mut directories, directory);
        }
    }
    for directory in SYSTEM_BIN_DIRECTORIES {
        push_unique(&mut directories, PathBuf::from(directory));
    }

    std::env::join_paths(&directories).unwrap_or_else(|_| {
        OsString::from("/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin")
    })
}

fn push_unique(directories: &mut Vec<PathBuf>, directory: PathBuf) {
    if !directory.as_os_str().is_empty() && !directories.contains(&directory) {
        directories.push(directory);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_codex_path_repairs_a_stripped_macos_gui_environment() {
        let path = local_codex_path(
            "/opt/homebrew/bin/codex",
            Some(OsStr::new("/usr/bin:/bin")),
            Some(OsStr::new("/Users/roadie")),
        );
        let directories: Vec<_> = std::env::split_paths(&path).collect();

        assert_eq!(
            directories.first(),
            Some(&PathBuf::from("/opt/homebrew/bin"))
        );
        assert!(directories.contains(&PathBuf::from("/usr/local/bin")));
        assert!(directories.contains(&PathBuf::from("/Users/roadie/.local/bin")));
        assert_eq!(
            directories
                .iter()
                .filter(|directory| directory.as_os_str() == "/usr/bin")
                .count(),
            1
        );
    }

    #[test]
    fn executable_directory_takes_priority_for_custom_installations() {
        let path = local_codex_path(
            "/Users/roadie/tools/bin/codex",
            Some(OsStr::new("/usr/bin:/bin")),
            None,
        );
        let directories: Vec<_> = std::env::split_paths(&path).collect();

        assert_eq!(
            directories.first(),
            Some(&PathBuf::from("/Users/roadie/tools/bin"))
        );
        assert!(directories.contains(&PathBuf::from("/opt/homebrew/bin")));
    }
}
