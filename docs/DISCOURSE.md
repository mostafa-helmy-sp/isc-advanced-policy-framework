![title-image|690x389](upload://vlzaNd95TBzWnvrHTdowE1G24He.png)

<div data-theme-toc="true"> </div>

> **Credits:** This framework was designed with the input and help of @Bassem_Mohamed

Hi SOD-savvy SailPoint customers,

This tool is an example that combines the [SaaS Connectivity Framework](https://developer.sailpoint.com/docs/connectivity/saas-connectivity/) with the [TypeScript SDK](https://developer.sailpoint.com/docs/tools/sdk/typescript/) to implement dynamic search-based policies for Identity Security Cloud (ISC) and automate the administration tasks required to implement separation-of-duty (SOD) policies and certification campaigns at scale.

You can find the connector source code on [this GitHub repository](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/) (current release: **1.0.2**). Detailed references live in the repo docs:

* [Architecture](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/docs/ARCHITECTURE.md)
* [Configuration](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/docs/CONFIGURATION.md)
* [Policy CSV](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/docs/POLICY-CSV.md)
* [API Reference](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/docs/API.md)
* [CHANGELOG](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/CHANGELOG.md)

This blog examines some of the ways you can use this tool to solve different business challenges.

## Dynamic SOD policy definitions

A common approach many organizations take to define their SOD policies and matrices is to do so on the entitlement metadata level rather than on the technical entitlement level.

For example:

* The organization business analysts define a list of toxic / conflicting business processes (e.g. Accounts Payable vs. Accounts Receivable).
* The business analysts later identify which technical entitlements match each business process.

The ideal implementation of such SOD models would include the following:

* A definition of Identity Security Cloud policies based on the conflicting business processes without manual specification of the technical entitlements in the policy definition.
* A separate ability to “tag” the technical entitlements that match these business processes - the policies would automatically pick up these “tagged” technical entitlements.

Out of the box, SOD policies in Identity Security Cloud are still defined by selecting entitlement lists for each conflicting side. That means you typically maintain those entitlement lists by editing the policy definition, rather than defining a dynamic policy that resolves entitlements through Search and tagging.

## Automating SOD policy certification campaign definitions

You can create certification campaigns from your SOD policies as an additional configuration step in Identity Security Cloud. While it’s a simple additional configuration step, it can take some time if you need to do it at scale with many policies.

Also, in certain cases, you will have to reconfigure the created certification campaigns when certain changes are made to their respective policies, which adds an additional administration cost.

## What’s new (through 1.0.2)

### 1.0.2
* HTTP **429 retries actually work** under the SailPoint TypeScript SDK (retries run in axios interceptors before the SDK converts errors to `ApiError`)
* Stronger rate-limit handling for large / parallel runs: up to **15** retries on 429, exponential backoff with jitter that honors `Retry-After`, and a shared cooldown after any 429
* Transient **5xx / network** errors retried up to 5 times for read-only and idempotent requests only (creates/updates are never retried on those errors)
* Clearer policy `errorMessages` (HTTP status, ISC error text, `trackingId`, labelled by query side or owner)
* One failing policy no longer aborts the whole aggregation
* Circular nested entitlement hierarchies no longer recurse forever
* `keepAlive` every 60 seconds during account list/read so long retry waits don’t hit the 3-minute SaaS command timeout
* Per-policy logging (`policyName` on processing lines; finish line at WARN when a policy has errors)

### 1.0.1
* Application-level HTTP 429 retry with `Retry-After` and exponential backoff
* Real API failure messages on policy `errorMessages` instead of misleading “returns no entitlements”
* Default `maxConcurrentPolicies` lowered from **10** to **3** to reduce rate-limit pressure

### 1.0.0
* Migrated to `sailpoint-api-client` **2.x** with per-service **v1** APIs
* CSV support for policy severity (`Level`) and pipe-delimited co-owners (`CoOwners`)
* Optional parallel policy processing, nested entitlement resolution, Search API ID batching, and configurable entitlement/campaign limits
* Expanded repo documentation (architecture, configuration, CSV reference, API notes)

## High-level Design

There are three main components to the solution:

![Highlevel-Design-Diagram|690x290](upload://uGbGGaL2K4LhoXahggG3p9Rmc7Y.png)

### Policy Configuration CSV File

* This is the master file that holds the required policy configurations.
* Each line represents a separate policy that the framework will auto configure and maintain.
* The policy configuration CSV file can be included in source control for versioning.

| **Field Name** | **Required** | **Description** | **Expected Value** |
|----|----|----|----|
| PolicyName | :white_check_mark: | Identity Security Cloud Policy name, acts as the main identifier | text |
| PolicyType | :white_check_mark: | The type of Policy. For now only SOD is supported | SOD |
| PolicyDescription | :x: | Identity Security Cloud Policy description | text |
| PolicyOwnerType | :white_check_mark: | Type of entity responsible for the policy configuration | IDENTITY / INDIVIDUAL / Individual, or GOVERNANCE_GROUP (and aliases) |
| PolicyOwner | :white_check_mark: | Name of the Identity or Governance Group responsible for the policy configuration | text |
| Level | :x: | Policy severity (defaults to HIGH if omitted) | LOW, MEDIUM, HIGH, CRITICAL |
| CoOwners | :x: | Additional policy co-owners (max 10). Empty value clears co-owners on update. | Pipe-delimited `TYPE:value` entries, e.g. `IDENTITY:jane.doe\|GOVERNANCE_GROUP:Accounting` |
| PolicyEnabled | :white_check_mark: | Whether the policy is enforced or not | yes/no/true/false |
| ExternalReference | :x: | Standard Identity Security Cloud Policy attribute | text |
| Tags | :x: | Standard Identity Security Cloud Policy attribute | Comma-separated list (no spaces) |
| Query1Name | :white_check_mark: | Display Name for the SOD Policy Left-hand query | text |
| Query1 | :white_check_mark: | The SOD Policy Left-hand query | Valid Identity Security Cloud Entitlements Search Query |
| Query2Name | :white_check_mark: | Display Name for the SOD Policy Right-hand query | text |
| Query2 | :white_check_mark: | The SOD Policy Right-hand query | Valid Identity Security Cloud Entitlements Search Query |
| ViolationOwnerType | :white_check_mark: | Type of entity responsible for violation remediation | IDENTITY / INDIVIDUAL / Individual, GOVERNANCE_GROUP, or MANAGER |
| ViolationOwner | :orange_circle: | Not required with MANAGER type | Name of the Identity or Governance Group responsible for violation remediation |
| MitigatingControls | :x: | Standard Identity Security Cloud Policy attribute | text |
| CorrectionAdvice | :x: | Standard Identity Security Cloud Policy attribute | text |
| Actions | :x: | Additional actions required aside from defining the policy. Empty action will only create a policy. | REPORT, CERTIFY, DELETE_ALL, DELETE_CAMPAIGN |
| PolicySchedule | :orange_circle: | Only with the REPORT action | DAILY, WEEKLY, MONTHLY |
| CertificationName | :orange_circle: | Only with the DELETE_CAMPAIGN / CERTIFY actions | text |
| CertificationDescription | :orange_circle: | Only with the CERTIFY action | text |
| CertificationSchedule | :x: | Optional with the CERTIFY action to auto-schedule the campaign | WEEKLY, MONTHLY |

See the [Policy CSV reference](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/docs/POLICY-CSV.md) for co-owner format details and sample rows. Older CSV files without `Level` / `CoOwners` remain compatible (`Level` defaults to HIGH; empty `CoOwners` clears co-owners on update).

### Delimited file / Generic CSV source

The delimited file (Generic CSV) source serves as a way to store the earlier policy configurations in Identity Security Cloud.

* This eliminates the need to externally store and read the policy configuration file.
* This also means that you can recover the current configuration file at any time by downloading the accounts CSV file from this source.

### SaaS connector and source

The solution uses the [SaaS Connectivity Framework](https://developer.sailpoint.com/docs/connectivity/saas-connectivity/) and leverages the [TypeScript SDK](https://developer.sailpoint.com/docs/tools/sdk/typescript/) (`sailpoint-api-client` 2.x) to automate all the required administration tasks.

The logic has been implemented in the account aggregation (`std:account:list` / `std:account:read`), which allows you to easily schedule these policy and campaign refresh tasks as required.

You can also configure a workflow to automatically trigger account aggregation on this source once an account aggregation finishes successfully on the delimited file source.

Optional connector settings include identity resolution attribute, schedule defaults, campaign duration, max entitlements per policy side, max access items per campaign, nested entitlement resolution, and parallel policy processing (`maxConcurrentPolicies` defaults to **3**). See the [Configuration docs](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/docs/CONFIGURATION.md) for rate-limit retry behavior.

## Processing logic

An administrator is expected to upload a policy configuration CSV file to the delimited file source in Identity Security Cloud. Once that’s done, the SaaS connector account aggregation performs these actions:

1. Find the delimited file “Advanced Policy Configurations” source by using the source name provided in the SaaS connector source configuration using the [List Sources API](https://developer.sailpoint.com/docs/api/v3/list-sources).

2. List all the policy configurations in the delimited file using the [List Accounts API](https://developer.sailpoint.com/docs/api/v3/list-accounts).

3. Process each policy configuration record as follows:

   * Use the [Search API](https://developer.sailpoint.com/docs/api/v3/search-post) to find entitlements matching both provided queries (large ID lists are batched).
   * Optionally expand nested entitlement relationships when that setting is enabled (circular hierarchies are detected and stopped).
   * Use the relevant APIs to resolve owners of type IDENTITY ([Search API](https://developer.sailpoint.com/docs/api/v3/search-post)) and GOVERNANCE_GROUP ([Governance Groups API](https://developer.sailpoint.com/docs/api/v2025/list-workgroups)).
   * Use the relevant [SOD policy APIs](https://developer.sailpoint.com/docs/api/v3/sod-policies) to find and patch an existing policy, or create a new one (including `level` and `secondaryOwnerRefs` when provided).
   * Schedule the policy using the [Update policy schedule API](https://developer.sailpoint.com/docs/api/v3/put-sod-policy-schedule) if the REPORT action was set.
   * Use the [Search API](https://developer.sailpoint.com/docs/api/v3/search-post) to find all access profiles that contain the entitlements matching both provided queries.
   * Use the [Search API](https://developer.sailpoint.com/docs/api/v3/search-post) to find all roles that contain the access profiles from the previous step.
   * Use the relevant [certification campaign template APIs](https://developer.sailpoint.com/docs/api/v3/get-campaign-templates) to find and patch an existing campaign or create a new one if the CERTIFY action was set.
   * Schedule the campaign by using the [set campaign template schedule API](https://developer.sailpoint.com/docs/api/v3/set-campaign-template-schedule) if the CERTIFY action and CertificationSchedule field were set.

4. Prepare a policy implementation status object to be returned as an account in the SaaS connector source. The “status” account would show details like these:

   * Indication of whether the policy, policy schedule, campaign, and campaign schedule have been configured
   * List of error messages faced during processing the policy configuration (including real HTTP / ISC error text when API calls fail after retries)
   * Statistics on the number of access items relevant to each side of the policy queries
   * Lists of relevant entitlements, access profiles and roles on either side of the policy queries

5. You can get a full processing status report by downloading the “Accounts” CSV file from the SaaS connector source.

When parallel processing is enabled, policies are processed concurrently (up to `maxConcurrentPolicies`, default **3**) with separate API clients / tokens. ISC still rate-limits per OAuth client, so all policies share one request budget. HTTP 429 responses are retried with exponential backoff (honoring `Retry-After`); long aggregations send `keepAlive` so they are not killed by the SaaS command timeout.

# Usage Instructions

> :warning: Always make sure you properly test and validate any configurations in your sandbox environment(s) first before moving them to production

## Configure your policy configuration CSV

Use the sample policy configuration CSV in GitHub (preferred).

### Things to consider:

* Ensure you have **unique names** for the different policies and campaigns where it’s relevant.
* The **policy name** is the **unique identifier** for policies. Changing a policy name will create a new policy rather than rename an existing policy.
* The **certification name** is the **unique identifier** for certifications. Changing a campaign name will create a new campaign rather than rename an existing campaign.
* Ensure that you use valid **[entitlement search queries](https://documentation.sailpoint.com/saas/help/search/searchable-fields.html#searching-entitlement-data)**.
  * It is recommended that you test the search queries in the Identity Security Cloud UI first to ensure their validity and that they return the required entitlements.
  * You can leverage the [Identity Security Cloud tagging capabilities](https://documentation.sailpoint.com/saas/help/common/tagging.html) to tag entitlements and then use the tag-based search query (`tags:YOUR_TAG`) for the ultimate flexibility of which entitlements to target.
* Ensure that you add the correct policy owner, co-owner, and violation manager references. Nothing will be configured if those references are broken.
* Use `Level` and `CoOwners` when you want severity and secondary owners set on the SOD policy.
* On large batches, start with serial processing or a low `maxConcurrentPolicies` (default is 3) if you see rate-limit errors; 1.0.2 retries 429s aggressively, but fewer concurrent policies still reduces pressure.

## Configure your delimited file source

1. Create a new delimited file / Generic CSV source within your tenant.
2. Configure the account schema by uploading the sample CSV file. Ensure that the PolicyName attribute is set as both the account ID and account name.
3. Import your policy configuration CSV under the `Account Aggregations` tab.

## Configure your SaaS connector source

You can find the source code and current pre-built packages on [this GitHub repository](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/) / [Releases](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/releases). Alternatively, you can use the connector zip attached in Resources below. **Note:** Prefer the latest release from GitHub Releases to ensure you have the most recent version.

1. Generate a personal access token (PAT) from an **org admin** identity in your tenant. Ensure that you add the `sp:scopes:all` scope so the connector can call all required APIs.
2. Follow [these instructions](https://developer.sailpoint.com/docs/tools/cli/) to install the SailPoint CLI.
3. Follow [these instructions](https://developer.sailpoint.com/docs/connectivity/saas-connectivity/test-build-deploy/) to optionally build and then deploy the SaaS connector.

> * If you are building the connector from the code, start at the [Package connector files](https://developer.sailpoint.com/docs/connectivity/saas-connectivity/test-build-deploy/#package-connector-files) step (`npm ci`, `npm run build`, `npm run pack-zip`, then upload). The package is written to `dist/advanced-policy-framework-<version>.zip`.
> * If you are using a pre-built connector package from GitHub Releases (or the attachment below), start at the [Create connector in your org](https://developer.sailpoint.com/docs/connectivity/saas-connectivity/test-build-deploy/#create-connector-in-your-org) step.

 4. Create a new source in your tenant using the newly added SaaS connector.
 5. Fill in the required details under **Connection Configurations**.
 6. Add your tenant API URL and PAT details (Client ID / Client Secret) to the source configuration. The connector authenticates via the standard OAuth token endpoint using those PAT credentials.
 7. Populate the name of the *delimited file source* created in the previous step in the **Policy Configuration Source Name** field.
 8. Optionally fill in the details under **Additional Settings** (resolution, scheduling, limits) and **Administrator Settings** (parallel processing / debug logging).
 9. Test the connection to ensure the required details are correct.
10. Run an account aggregation on this SaaS connector source and witness the magic! :wink:
11. In ideal situations, schedule the account aggregation so that it periodically refreshes the configured policies and campaigns with any new updates either to the policy configurations or the entitlement data.

## Resources

> :warning: Prefer [GitHub Releases](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/releases) for the latest connector package.

### INSERT ATTACHMENT HERE

Current sample CSV in GitHub: [SamplePolicies.csv](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/blob/main/SamplePolicies.csv)
