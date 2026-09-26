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
| `maxConcurrentPolicies` | `3` | Max policies processed simultaneously when parallel processing is enabled |
| `resolveNestedEntitlements` | `false` | Expand entitlement queries to include hierarchy members |

## Rate limiting and retries

ISC allows 100 requests per 10 seconds per OAuth client, so large or parallel runs regularly receive HTTP 429 responses. Every SDK call goes through one axios instance ([`src/api/axios-handlers.ts`](../src/api/axios-handlers.ts)), which:

- Retries HTTP 429 up to 15 times for any request. Each wait honors `Retry-After` and adds exponential backoff (2 second base, doubling, capped at 60 seconds) with jitter that widens on every attempt, so requests that keep colliding spread out across later rate-limit windows.
- After any 429, holds every outgoing request until `Retry-After` has passed, so the connector stops sending requests into a window it already knows is exhausted.
- Retries 5xx and network errors up to 5 times, but only for read-only or idempotent requests (GET, PUT, DELETE, and Search). Creates and updates are not retried on these errors, because the change may already have been applied.
- Times out requests after 120 seconds. Timeouts are not retried.
- Logs each retry at INFO, and each request's method, path, status, and duration at DEBUG.

When retries are exhausted, the policy's `errorMessages` shows the HTTP status and the ISC error text, for example `HTTP 429 Too Many Requests: Rate Limit Exceeded (gave up after 15 retries)`.

Account list and account read call `keepAlive` every 60 seconds, so the 3-minute SaaS command timeout does not interrupt long retry waits.
