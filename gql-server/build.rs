// 2022-2026 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
use std::process::Command;

trait OutputStdout {
    fn stdout_or(&mut self, default: &str) -> String;
}

impl OutputStdout for Command {
    fn stdout_or(&mut self, default: &str) -> String {
        self.output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .unwrap_or(default.to_string())
    }
}

macro_rules! cmd {
    ($c:expr, $($h:expr),*) => { Command::new($c).env_clear() $( .arg($h) )* };
}

fn main() {
    cmd!["git", "--version"].output().expect("Make sure `git` is installed");

    let unknown = "Unknown";
    let git_commit = cmd!["git", "rev-parse", "HEAD"].stdout_or(unknown);

    println!("cargo:rustc-env=BUILD_GIT_COMMIT={git_commit}");
}
