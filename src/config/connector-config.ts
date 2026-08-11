import {
    DEFAULT_CAMPAIGN_DURATION,
    DEFAULT_HOURLY_SCHEDULE_DAY,
    DEFAULT_IDENTITY_RESOLUTION_ATTRIBUTE,
    DEFAULT_MAX_ACCESS_ITEMS_PER_CAMPAIGN,
    DEFAULT_MAX_CONCURRENT_POLICIES,
    DEFAULT_MAX_ENTITLEMENTS_PER_POLICY_SIDE,
    DEFAULT_MONTHLY_SCHEDULE_DAY,
    DEFAULT_WEEKLY_SCHEDULE_DAY,
} from './defaults'

/**
 * Connector source configuration as defined in connector-spec.json.
 * Values are populated by the SailPoint Connector SDK via readConfig().
 */
export interface ConnectorConfig {
    apiUrl: string
    clientId: string
    clientSecret: string
    policyConfigSourceName: string
    identityResolutionAttribute?: string
    hourlyScheduleDay?: string | string[]
    weeklyScheduleDay?: string | string[]
    monthlyScheduleDay?: string | string[]
    campaignDuration?: string
    maxEntitlementsPerPolicySide?: number
    maxAccessItemsPerCampaign?: number
    parallelProcessing?: boolean
    maxConcurrentPolicies?: number
    resolveNestedEntitlements?: boolean
}

/** Resolved connector settings with defaults applied. */
export interface ResolvedConnectorSettings {
    policyConfigSourceName: string
    identityResolutionAttribute: string
    hourlyScheduleDay: string[]
    weeklyScheduleDay: string[]
    monthlyScheduleDay: string[]
    campaignDuration: string
    maxEntitlementsPerPolicySide: number
    maxAccessItemsPerCampaign: number
    parallelProcessing: boolean
    maxConcurrentPolicies: number
    resolveNestedEntitlements: boolean
}

export function resolveConnectorSettings(config: ConnectorConfig): ResolvedConnectorSettings {
    return {
        policyConfigSourceName: config.policyConfigSourceName,
        identityResolutionAttribute: config.identityResolutionAttribute ?? DEFAULT_IDENTITY_RESOLUTION_ATTRIBUTE,
        hourlyScheduleDay: normalizeStringArray(config.hourlyScheduleDay, DEFAULT_HOURLY_SCHEDULE_DAY),
        weeklyScheduleDay: normalizeStringArray(config.weeklyScheduleDay, DEFAULT_WEEKLY_SCHEDULE_DAY),
        monthlyScheduleDay: normalizeStringArray(config.monthlyScheduleDay, DEFAULT_MONTHLY_SCHEDULE_DAY),
        campaignDuration: config.campaignDuration || DEFAULT_CAMPAIGN_DURATION,
        maxEntitlementsPerPolicySide: config.maxEntitlementsPerPolicySide || DEFAULT_MAX_ENTITLEMENTS_PER_POLICY_SIDE,
        maxAccessItemsPerCampaign: config.maxAccessItemsPerCampaign || DEFAULT_MAX_ACCESS_ITEMS_PER_CAMPAIGN,
        parallelProcessing: config.parallelProcessing || false,
        maxConcurrentPolicies: config.maxConcurrentPolicies || DEFAULT_MAX_CONCURRENT_POLICIES,
        resolveNestedEntitlements: config.resolveNestedEntitlements || false,
    }
}

function normalizeStringArray(value: string | string[] | undefined, fallback: string[]): string[] {
    if (!value) {
        return fallback
    }
    return Array.isArray(value) ? value : [value]
}
