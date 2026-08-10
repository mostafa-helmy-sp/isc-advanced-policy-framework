# Changelog

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
