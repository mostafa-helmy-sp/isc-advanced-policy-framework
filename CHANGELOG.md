# Changelog

## 1.0.0 - 2026-08-11

First stable release after the SDK 2.0 modernization and modular refactor (development releases were tracked as 0.13.0–0.14.0).

### Highlights
- Migrated to `sailpoint-api-client` 2.x with per-service v1 APIs
- Refactored monolithic client into focused service modules with typed configuration
- CSV support for policy `Level` and pipe-delimited `CoOwners`
- Unit tests, GitHub Actions CI, and expanded documentation

### Performance
- Parallelize independent API calls within each policy (entitlement searches, owner resolution, access profiles, roles)
- Session-scoped owner and governance-group member lookup cache for batch aggregations
- Bounded parallel policy processing via `maxConcurrentPolicies` (default 10)
- Batched Search API queries for large entitlement and access profile ID lists (50 IDs per batch)

See 0.13.0 and 0.14.0 entries below for detailed change history from the pre-release period.

## 0.14.0 - 2026-08-10

### Added
- CSV support for policy `Level` (LOW, MEDIUM, HIGH, CRITICAL) mapped to the SOD Policies API `level` field.
- CSV support for `CoOwners` as pipe-delimited `TYPE:value` entries mapped to `secondaryOwnerRefs` (max 10).
- Owner type aliases: `Individual` / `INDIVIDUAL` normalize to `IDENTITY`; governance group variants normalize to `GOVERNANCE_GROUP`.

### Changed
- Upgraded `sailpoint-api-client` to `^2.1.16` (required for `level` and `secondaryOwnerRefs` on SOD policies).
- Refactored owner resolution into shared lookup used by policy owner, violation owner, and co-owners.

## 0.13.0 - 2026-08-10

### Changed
- Migrated from `sailpoint-api-client` 1.x (V2025 APIs) to 2.x (per-service v1 APIs).
- Refactored the monolithic `isc-client.ts` into focused service modules under `src/services/`.
- Removed `apiConfig.experimental = true`; v1 APIs are public and no longer require the experimental header.
- Added typed connector configuration via `ConnectorConfig` and centralized SDK imports in `src/types/sailpoint-api.ts`.

### Fixed
- DELETE_ALL error message now references `policyName` instead of `certificationName`.
- `campaignTemplateName` output attribute is now populated when a campaign is created or updated.
- Entitlement hierarchy cache moved from module scope to instance scope on `EntitlementHierarchyService`.

### Added
- Unit tests for policy parsing, schedule builders, access constraints, and connector defaults.
- GitHub Actions CI workflow (typecheck, test, build).
- Documentation: architecture, configuration, policy CSV reference, and API/OAuth scopes.
- Explicit `axios-retry` dependency.
