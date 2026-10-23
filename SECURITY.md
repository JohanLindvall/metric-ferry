# Security policy

## Reporting a vulnerability

Use [GitHub private vulnerability reporting](https://github.com/JohanLindvall/metric-ferry/security/advisories/new)
when it is available. If private reporting is unavailable, open an issue asking
the maintainer for a private reporting channel, without including vulnerability
details or an exploit.

In the private report, include the affected commit or version, impact, steps to
reproduce, and a minimal synthetic input if applicable. Do not include real
credentials, private receiver URLs, or production metrics.

## Supported code

Security fixes currently target the latest code on `main`. There are no separate
long-term support branches. Use `go version -m ./metric-ferry` to identify the
version and commit in a binary when reporting a problem.

## Import considerations

Metric Ferry sends HTTP requests to endpoints explicitly supplied by the caller.
Use trusted endpoints and HTTPS for remote receivers. Redirects are not followed.
The importer bounds lines, decoder memory, series state, and request sizes; take
care when increasing these limits for an untrusted archive.

A dry run validates and encodes without network writes. It does not establish
whether a receiver will accept an import. Previously accepted batches remain
accepted if a later batch fails; see the [delivery semantics](docs/usage.md#throughput-and-delivery).
