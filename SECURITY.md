# Security notes

The migration audit found no committed credentials in reachable history. This is a focused review, not a complete security audit.

## Dependency advisory

`sqlx-mysql 0.8.6` depends on `rsa 0.9.10`, which is affected by [RUSTSEC-2023-0071](https://rustsec.org/advisories/RUSTSEC-2023-0071.html). The advisory currently lists no patched version. The SQLx authentication implementation uses `RsaPublicKey::encrypt`, not private-key decryption; the private-key timing attack described by the advisory does not appear reachable through this usage. The dependency still triggers `cargo audit` and remains tracked rather than silently ignored.

Use certificate-verified TLS for production database connections. The integration-test URL deliberately disables TLS for its local disposable database only.

Live database tests are opt-in: set `BREEZE_MYSQL_TEST_URL` and enable `integration-tests`. Default CI runs unit and documentation tests without contacting a database.
