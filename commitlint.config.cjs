// Commit messages follow Conventional Commits (https://conventionalcommits.org),
// enforced locally by husky's commit-msg hook and by CI where it runs.
//
//   <type>(<scope>)!: <subject>
//
// Types are the conventional set; scopes mirror the source tree so history can
// be filtered per subsystem.
module.exports = {
    extends: ["@commitlint/config-conventional"],
    rules: {
        "header-max-length": [2, "always", 100],
        "body-max-line-length": [2, "always", 100],
        "footer-max-line-length": [2, "always", 100],
        // A guide, not a gate: an unfamiliar scope is a warning, not a failure.
        "scope-enum": [
            1,
            "always",
            [
                "storage",
                "broker",
                "proto",
                "net",
                "util",
                "ctl",
                "raft",
                "repl",
                "cluster",
                "tools",
                "tests",
                "examples",
                "deploy",
                "docker",
                "build",
                "ci",
                "docs",
                "deps",
            ],
        ],
    },
};
