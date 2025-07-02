// 2022-2024 (c) Copyright Contributors to the GOSH DAO. All rights reserved.
//

use std::process::Command;

trait OutputStdout {
    fn get_stdout(&mut self) -> String;
}

impl OutputStdout for Command {
    fn get_stdout(&mut self) -> String {
        self.output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .expect("Failed to get build variable")
    }
}

macro_rules! cmd {
    ($c:expr, $($h:expr),*) => { Command::new($c).env_clear() $( .arg($h) )* };
}

fn main() {
    let git_branch = cmd!["git", "rev-parse", "--abbrev-ref", "HEAD"].get_stdout();
    let git_commit = cmd!["git", "rev-parse", "HEAD"].get_stdout();
    let commit_date = cmd!["git", "log", "-1", "--date=iso", "--pretty=format:%cd"].get_stdout();
    let build_time = cmd!["date", "+%Y-%m-%d %T %z"].get_stdout();

    println!("cargo:rustc-env=BUILD_GIT_BRANCH={git_branch}");
    println!("cargo:rustc-env=BUILD_GIT_COMMIT={git_commit}");
    println!("cargo:rustc-env=BUILD_GIT_DATE={commit_date}");
    println!("cargo:rustc-env=BUILD_TIME={build_time}");
}
