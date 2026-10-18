# Security policy

Helix MQ is pre-1.0 software. Security fixes target the current development branch; older releases
do not have a guaranteed backport window or response SLA.

The broker currently has no authentication or TLS. Run it only on trusted, access-controlled
networks. Cluster ports must also be protected. Do not expose it directly to the public Internet.

Report suspected vulnerabilities through the repository's **Security → Report a vulnerability**
page at https://github.com/j4flmao/helix-mq/security/advisories/new when private reporting is enabled.
If that option is unavailable, ask the maintainer for a private reporting channel without posting
exploit details publicly. Include affected version, reproduction, impact, logs without secrets,
and any proposed fix. Coordinate disclosure with the maintainer.

CodeQL, sanitizers, fuzzing and checksums reduce risk but are not a security certification.
Verify release checksums and GitHub artifact attestations before deployment.
