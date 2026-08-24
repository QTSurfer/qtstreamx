# Contributing to QTStreamX

1. Open an issue for a substantial change and explain the compatibility impact.
2. Create a focused branch and keep changes scoped to one concern.
3. Run `./gradlew clean build` and the relevant module tests. `build` depends on
   `check`, which verifies the publication boundary, the dependency/license
   inventory, and the version — so a green build covers them.
4. Changing a third-party dependency also means updating the matching table in
   `THIRD_PARTY_LICENSES.md`; `check` fails naming the exact coordinate until
   the two agree.
5. Update public documentation and `CHANGELOG.md` when the user-visible API or
   release behavior changes.
6. Open a pull request with test evidence and any release/platform limitations.

Automated contributors should also read [AGENTS.md](AGENTS.md).

Contributions must be original work or clearly identified third-party work
compatible with Apache License 2.0. Do not include credentials, private
endpoints, captured market data, or generated build output.
