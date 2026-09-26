# Changelog

## 1.0.2 - 2026-09-26

### Fixed
- HTTP 429 retries now actually happen. The SDK replaces every axios error with an `ApiError` that has no `response` or headers, so the 1.0.1 retry check never matched (73 unretried 429s in a customer run). Retries now run in axios interceptors on the SDK's `Configuration.axiosInstance`, before that conversion.
- Retries are tuned for high concurrency:
  - up to 15 retries for 429
  - exponential backoff with widening jitter that honors `Retry-After`
  - a shared cooldown that holds all requests after any 429
- Transient 5xx and network errors are retried up to 5 times, for read-only and idempotent requests only. Creates and updates are never retried on these errors.
- Policy `errorMessages` now show the HTTP status, ISC error text, and `trackingId`, labelled by query side or owner, e.g. `Entitlement Query 1 [...]: Error finding entitlements using Search API: HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)`.
- An unexpected exception while processing one policy is recorded on that policy instead of aborting the aggregation.
- Circular entitlement nesting no longer recurses forever when nested entitlement resolution is enabled.

### Changed
- Lines logged while processing a policy include a `policyName` field.
- Every policy logs a finish line, at WARN with its errors when it has any.
- Account list and account read call `keepAlive` every 60 seconds, so long retry waits don't hit the 3-minute SaaS command timeout.
- API requests time out after 120 seconds.
- Added `axios` as a direct dependency.
- Enabled `isolatedModules` in `tsconfig.json`, which ts-jest requires with `module: Node16`.

## 1.0.1 - 2026-09-21

### Fixed
- Replaced ineffective SDK `axios-retry` / `retriesConfig` with application-level HTTP 429 retry (honors `Retry-After`, exponential backoff, INFO logging).
- Surface real API failure messages on policy `errorMessages` instead of collapsing failed searches into misleading "returns no entitlements" errors. Applies across Search, owner resolution, SOD policy lookup/create/update, campaign lookup/create/update, entitlement hierarchy, and governance-group member lookup.

### Changed
- Default `maxConcurrentPolicies` lowered from 10 to 3 to reduce rate-limit pressure on large batch aggregations.
- Removed unused `axios-retry` dependency.

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
