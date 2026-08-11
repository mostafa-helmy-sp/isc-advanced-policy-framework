# Architecture

## Overview

The Advanced Policy Framework is a SailPoint ISC connector that reads SOD policy definitions from a Generic CSV source and provisions policies, violation report schedules, and certification campaign templates in Identity Security Cloud.

## Data flow

```
Generic CSV Source (policy rows as accounts)
        │
        ▼
  std:account:list / std:account:read
        │
        ▼
     IscClient (facade)
        │
        ├── PolicyConfigSourceService   → Sources API, Accounts API
        ├── SearchService               → Search API
        ├── EntitlementHierarchyService → Sources API, Entitlements API
        ├── OwnerResolverService        → Search API, Governance Groups API
        ├── SodPolicyService            → SOD Policies API
        └── CampaignService             → Certification Campaigns API
        │
        ▼
  PolicyImpl (aggregation account attributes)
```

## Connector commands

| Command | Purpose |
|---------|---------|
| `std:test-connection` | Validates OAuth credentials and resolves the policy configuration source |
| `std:account:list` | Reads all policy rows and processes each SOD policy |
| `std:account:read` | Processes a single policy by name |

## Parallel processing

When `parallelProcessing` is enabled, policies are processed concurrently up to `maxConcurrentPolicies` (default 10). Each active policy receives its own SDK `Configuration` instance to reduce OAuth token contention and 429 rate-limit errors during large batch runs.

Within each policy, independent API calls (entitlement queries, owner resolution, access profile and role searches) run in parallel where safe.

## Caching

`OwnerResolverService` maintains a session-scoped cache for owner and governance-group member lookups for the duration of an aggregation run. This avoids repeated API calls when many policies share the same owners.

## Search query batching

Large entitlement or access profile ID lists are split into batches of 50 IDs per Search API query. Results are merged and deduplicated before use in policy or campaign configuration.

## Module map

| Path | Responsibility |
|------|----------------|
| `src/index.ts` | Connector SDK entry point |
| `src/isc-client.ts` | Orchestrates policy processing workflow |
| `src/services/` | ISC API service classes |
| `src/builders/` | Pure schedule and access-constraint builders |
| `src/config/` | Connector settings and defaults |
| `src/types/sailpoint-api.ts` | SDK v2 import barrel |
