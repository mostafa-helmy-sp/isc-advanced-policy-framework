# API Reference

This connector uses the SailPoint TypeScript SDK (`sailpoint-api-client` 2.x) with per-service v1 APIs.

## APIs used

| Service | SDK class | Operations |
|---------|-----------|------------|
| Sources | `SourcesApi` | `listSourcesV1`, `getSourceSchemasV1` |
| Accounts | `AccountsApi` | `listAccountsV1` |
| Search | `SearchApi` | `searchPostV1` (via `Paginator.paginateSearchApi`) |
| Entitlements | `EntitlementsApi` | `listEntitlementChildrenV1`, `listEntitlementParentsV1` |
| Governance Groups | `GovernanceGroupsApi` | `listWorkgroupsV1`, `listWorkgroupMembersV1` |
| SOD Policies | `SODPoliciesApi` | `listSodPoliciesV1`, `createSodPolicyV1`, `patchSodPolicyV1`, `deleteSodPolicyV1`, `putPolicyScheduleV1` |
| Certification Campaigns | `CertificationCampaignsApi` | `getCampaignTemplatesV1`, `createCampaignTemplateV1`, `patchCampaignTemplateV1`, `deleteCampaignTemplateV1`, `setCampaignTemplateScheduleV1` |

## Recommended OAuth scopes

Configure your ISC OAuth client with scopes that grant access to the APIs above. At minimum:

| Scope area | Typical scopes |
|------------|----------------|
| Sources | Read sources and schemas |
| Accounts | Read accounts from the policy configuration source |
| Search | Execute search queries |
| Entitlements | Read entitlement hierarchy |
| Workgroups | Read governance groups and members |
| SOD Policies | Read, create, update, delete SOD policies and schedules |
| Certification Campaigns | Read, create, update, delete campaign templates and schedules |

Exact scope names depend on your tenant's OAuth client configuration. Use the ISC Admin panel to assign scopes matching the API categories listed above.

## Pagination and retry

- List and search operations use the SDK `Paginator` helper (250 items per page by default).
- HTTP 429 responses are retried up to 10 times with exponential backoff.

## Migration from V2025

This connector migrated from `sailpoint-api-client` 1.x (year-based V2025 APIs) to 2.x (per-service v1 APIs) in version 0.13.0. Legacy V2025 endpoints remain available until Q1 2029 per SailPoint's deprecation schedule.

See the [SailPoint API versioning migration guide](https://developer.sailpoint.com/docs/api/api-versioning-migration) for details.

## Manual validation checklist

After deployment, validate against a dev tenant:

1. `std:test-connection`
2. `std:account:list` (serial and parallel modes)
3. `std:account:read` for a single policy
4. Each action path: `REPORT`, `CERTIFY`, `DELETE_ALL`, `DELETE_CAMPAIGN`
