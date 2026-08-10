# Policy CSV Reference

Policy definitions are stored as accounts in a Generic CSV source. See [`SamplePolicies.csv`](../SamplePolicies.csv) for examples.

## Columns

| Column | Required | Description |
|--------|----------|-------------|
| `PolicyName` | Yes | Unique policy name (also used as connector account identity) |
| `PolicyType` | Yes | Policy type; only `SOD` is processed today |
| `PolicyDescription` | No | Policy description |
| `PolicyOwnerType` | Yes | `IDENTITY` or `GOVERNANCE_GROUP` |
| `PolicyOwner` | Yes | Owner identity attribute value or governance group name |
| `PolicyEnabled` | Yes | `true`/`yes` or `false`/`no` — maps to ENFORCED / NOT_ENFORCED |
| `ExternalReference` | No | External reference URL or ID |
| `Tags` | No | Comma-separated policy tags |
| `Query1Name` | Yes | Display name for left-hand entitlement query |
| `Query1` | Yes | ISC search query for left-hand entitlements |
| `Query2Name` | Yes | Display name for right-hand entitlement query |
| `Query2` | Yes | ISC search query for right-hand entitlements |
| `ViolationOwnerType` | Yes | `IDENTITY`, `GOVERNANCE_GROUP`, or `MANAGER` |
| `ViolationOwner` | Conditional | Required unless type is `MANAGER` |
| `MitigatingControls` | No | Compensating controls text |
| `CorrectionAdvice` | No | Correction advice text |
| `Actions` | No | Comma-separated: `REPORT`, `CERTIFY`, `DELETE_ALL`, `DELETE_CAMPAIGN` |
| `PolicySchedule` | Conditional | `DAILY`, `WEEKLY`, or `MONTHLY` — required when `REPORT` action is set |
| `CertificationName` | Conditional | Campaign template name — required for `CERTIFY` or delete actions |
| `CertificationDescription` | Conditional | Campaign description — required for `CERTIFY` |
| `CertificationSchedule` | No | `WEEKLY` or `MONTHLY` campaign template schedule |

## Actions

| Action | Behavior |
|--------|----------|
| `REPORT` | Creates/updates the SOD policy and configures a violation report schedule |
| `CERTIFY` | Creates/updates a certification campaign template linked to the policy query |
| `DELETE_ALL` | Deletes the SOD policy by name |
| `DELETE_CAMPAIGN` | Deletes the certification campaign template by name |

## Search query examples

```
name:"AccountingGeneral" AND source.name:"Active Directory"
tags:SOX_AP
@entitlements(id:abc123 OR id:def456)
```

Refer to the [ISC Search API documentation](https://developer.sailpoint.com/docs/api/search/) for full query syntax.
