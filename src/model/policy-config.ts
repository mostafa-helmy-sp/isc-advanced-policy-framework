import { Account } from '../types/sailpoint-api'
import { normalizeOwnerType, normalizeViolationOwnerType } from '../utils/owner-parser'

/** CSV account object from the policy configuration source. */
export interface PolicyConfigAccount {
    attributes: Record<string, string | undefined>
}

export function toPolicyConfigAccount(account: Account): PolicyConfigAccount {
    const attributes: Record<string, string | undefined> = {}
    if (account.attributes) {
        for (const [key, value] of Object.entries(account.attributes)) {
            attributes[key] = value != null ? String(value) : undefined
        }
    }
    return { attributes }
}

/**
 * Domain model parsed from a policy configuration CSV row.
 * Field names map to CSV column headers defined in SamplePolicies.csv.
 */
export class PolicyConfig {
    /** CSV column: PolicyName */
    policyName: string
    /** CSV column: PolicyType (only SOD is processed) */
    policyType: string
    /** CSV column: PolicyDescription */
    policyDescription?: string
    /** CSV column: PolicyOwnerType (Identity, GovernanceGroup, etc.) */
    policyOwnerType: string
    /** CSV column: PolicyOwner — resolved via identity or workgroup search */
    policyOwner: string
    /** CSV column: Level — LOW, MEDIUM, HIGH, or CRITICAL */
    level: string
    /** CSV column: CoOwners — pipe-delimited TYPE:value entries */
    coOwners: string
    /** CSV column: PolicyEnabled — true, yes, false, or omitted */
    policyState: boolean
    /** CSV column: ExternalReference */
    externalReference?: string
    /** CSV column: Tags — comma-separated */
    tags: string[]
    /** CSV column: Query1Name — left-hand criteria label */
    query1Name: string
    /** CSV column: Query1 — search query for left-hand entitlements */
    query1: string
    /** CSV column: Query2Name — right-hand criteria label */
    query2Name: string
    /** CSV column: Query2 — search query for right-hand entitlements */
    query2: string
    /** CSV column: ViolationOwnerType */
    violationOwnerType: string
    /** CSV column: ViolationOwner */
    violationOwner: string
    /** CSV column: MitigatingControls */
    mitigatingControls: string
    /** CSV column: CorrectionAdvice */
    correctionAdvice: string
    /** CSV column: Actions — comma-separated PolicyAction values */
    actions: string[]
    /** CSV column: PolicySchedule — DAILY, WEEKLY, or MONTHLY */
    policySchedule: string
    /** CSV column: CertificationName — campaign template name */
    certificationName: string
    /** CSV column: CertificationDescription */
    certificationDescription: string
    /** CSV column: CertificationSchedule — WEEKLY or MONTHLY */
    certificationSchedule: string

    constructor(object: PolicyConfigAccount) {
        const attrs = object.attributes
        this.policyName = attrs.PolicyName ?? ''
        this.policyType = attrs.PolicyType ?? ''
        this.policyDescription = attrs.PolicyDescription
        this.policyOwnerType = normalizeOwnerType(attrs.PolicyOwnerType ?? '') ?? attrs.PolicyOwnerType ?? ''
        this.policyOwner = attrs.PolicyOwner ?? ''
        this.level = attrs.Level ?? ''
        this.coOwners = attrs.CoOwners ?? ''
        this.policyState = parsePolicyEnabled(attrs.PolicyEnabled)
        this.externalReference = attrs.ExternalReference
        this.tags = attrs.Tags ? attrs.Tags.split(',') : []
        this.query1 = attrs.Query1 ?? ''
        this.query2 = attrs.Query2 ?? ''
        this.query1Name = attrs.Query1Name ?? ''
        this.query2Name = attrs.Query2Name ?? ''
        this.violationOwnerType = normalizeViolationOwnerType(attrs.ViolationOwnerType ?? '')
        this.violationOwner = attrs.ViolationOwner ?? ''
        this.mitigatingControls = attrs.MitigatingControls ?? ''
        this.correctionAdvice = attrs.CorrectionAdvice ?? ''
        this.actions = attrs.Actions ? attrs.Actions.split(',') : []
        this.policySchedule = attrs.PolicySchedule ?? ''
        this.certificationName = attrs.CertificationName ?? ''
        this.certificationDescription = attrs.CertificationDescription ?? ''
        this.certificationSchedule = attrs.CertificationSchedule ?? ''
    }
}

export function parsePolicyEnabled(value: string | undefined): boolean {
    if (!value) {
        return false
    }
    const normalized = value.toLocaleLowerCase()
    return normalized === 'true' || normalized === 'yes'
}
