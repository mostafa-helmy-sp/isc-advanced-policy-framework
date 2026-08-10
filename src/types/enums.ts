/** Policy types supported by the connector. Only SOD is processed today. */
export enum PolicyType {
    SOD = 'SOD',
}

/**
 * Actions that can be applied when processing a policy configuration row.
 * - REPORT: configure a violation report schedule on the SOD policy
 * - CERTIFY: create or update a certification campaign template
 * - DELETE_ALL: delete the SOD policy (and optionally its campaign)
 * - DELETE_CAMPAIGN: delete the certification campaign template only
 */
export enum PolicyAction {
    REPORT = 'REPORT',
    CERTIFY = 'CERTIFY',
    DELETE_ALL = 'DELETE_ALL',
    DELETE_CAMPAIGN = 'DELETE_CAMPAIGN',
}

/** Direction of nested entitlement resolution for a source schema. */
export enum EntitlementHierarchy {
    CHILD = 'CHILD',
    PARENT = 'PARENT',
    NONE = 'NONE',
}

/** DTO type strings used in owner and recipient references. */
export enum DtoType {
    Identity = 'IDENTITY',
    GovernanceGroup = 'GOVERNANCE_GROUP',
    Manager = 'MANAGER',
}
