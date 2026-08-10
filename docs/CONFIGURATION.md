# Configuration

All settings are defined in [`connector-spec.json`](../connector-spec.json) and resolved in [`src/config/connector-config.ts`](../src/config/connector-config.ts).

## Required settings

| Key | Description |
|-----|-------------|
| `apiUrl` | ISC tenant API URL, e.g. `https://tenant.api.identitynow.com` |
| `clientId` | OAuth 2.0 client ID |
| `clientSecret` | OAuth 2.0 client secret |
| `policyConfigSourceName` | Name of the Generic CSV source containing policy definitions |

## Optional settings

| Key | Default | Description |
|-----|---------|-------------|
| `identityResolutionAttribute` | `name` | Identity attribute used to resolve policy owners and violation managers |
| `hourlyScheduleDay` | `['9']` | Hour values for scheduled reports and campaigns |
| `weeklyScheduleDay` | `['MON']` | Day-of-week values for weekly schedules |
| `monthlyScheduleDay` | `['1']` | Day-of-month values for monthly schedules |
| `campaignDuration` | `P2W` | ISO-8601 duration for campaign deadline |
| `maxEntitlementsPerPolicySide` | `400` | Max entitlements per policy query side |
| `maxAccessItemsPerCampaign` | `10000` | Max total access items in a certification campaign |
| `parallelProcessing` | `false` | Process policies concurrently with separate API clients |
| `resolveNestedEntitlements` | `false` | Expand entitlement queries to include hierarchy members |

## Rate limiting

The SDK client retries HTTP 429 responses up to 10 times with exponential backoff (2 second base delay). See [`src/api/client-factory.ts`](../src/api/client-factory.ts).
