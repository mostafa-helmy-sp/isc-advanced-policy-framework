[![Discourse Topics][discourse-shield]][discourse-url]
[![Issues][issues-shield]][issues-url]
[![Latest Releases][release-shield]][release-url]
[![Contributor Shield][contributor-shield]][contributors-url]

[discourse-shield]:https://img.shields.io/discourse/topics?label=Discuss%20This%20Tool&server=https%3A%2F%2Fdeveloper.sailpoint.com%2Fdiscuss
[discourse-url]:https://developer.sailpoint.com/discuss/t/identity-security-cloud-advanced-policy-management-framework/13792
[issues-shield]:https://img.shields.io/github/issues/mostafa-helmy-sp/isc-advanced-policy-framework?label=Issues
[issues-url]:https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/issues
[release-shield]: https://img.shields.io/github/v/release/mostafa-helmy-sp/isc-advanced-policy-framework?label=Current%20Release
[release-url]:https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/releases
[contributor-shield]:https://img.shields.io/github/contributors/mostafa-helmy-sp/isc-advanced-policy-framework?label=Contributors
[contributors-url]:https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/graphs/contributors

# ISC Advanced Policy Management Framework

A SailPoint Identity Security Cloud connector that reads SOD policy definitions from a Generic CSV source and provisions policies, violation report schedules, and certification campaign templates.

[Explore the community discussion »](https://developer.sailpoint.com/discuss/t/identity-security-cloud-advanced-policy-management-framework/13792)

## Overview

Each row in the policy CSV source represents one SOD policy configuration. The connector:

1. Reads policy rows from a Generic CSV source via the Accounts API
2. Resolves entitlements, access profiles, and roles using the Search API
3. Creates or updates SOD policies via the SOD Policies API
4. Optionally schedules violation reports and certification campaigns

See [Architecture](docs/ARCHITECTURE.md) for the full data flow.

## Prerequisites

- Node.js 18+
- [SailPoint Connector CLI (`spcx`)](https://developer.sailpoint.com/discuss/t/about-the-sailpoint-developer-community-colab/11230)
- ISC tenant with an OAuth client configured for the required API scopes (see [API Reference](docs/API.md))

## Quick start

**Pre-built:** download `advanced-policy-framework-1.0.0.zip` from [Releases](https://github.com/mostafa-helmy-sp/isc-advanced-policy-framework/releases) and upload it when creating the SaaS connector in your ISC org.

**Build yourself:**

```bash
npm ci
npm run build
npm run pack-zip
```

The package is written to `dist/advanced-policy-framework-<version>.zip`. Deploy it to ISC and configure the source using `connector-spec.json`.

## Documentation

| Document | Description |
|----------|-------------|
| [Architecture](docs/ARCHITECTURE.md) | Data flow, commands, module map |
| [Configuration](docs/CONFIGURATION.md) | Source settings and defaults |
| [Policy CSV](docs/POLICY-CSV.md) | CSV column reference and actions |
| [API Reference](docs/API.md) | ISC APIs, OAuth scopes, validation checklist |
| [CHANGELOG](CHANGELOG.md) | Release history |

## Development

```bash
npm run typecheck   # TypeScript validation
npm test            # Unit tests with coverage
npm run build       # Bundle connector with ncc
npm run dev         # Run locally with spcx
```

## Contributing

Contributions are welcome. Please fork the repo and open a pull request.

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

Distributed under the MIT License. See `LICENSE.txt` for more information.

## Discuss

[Community discussion thread](https://developer.sailpoint.com/discuss/t/identity-security-cloud-advanced-policy-management-framework/13792)
