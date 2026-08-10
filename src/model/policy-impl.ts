import { Attributes, Key, SimpleKey, StdAccountListOutput, StdAccountReadOutput } from '@sailpoint/connector-sdk'

export class PolicyImpl implements StdAccountListOutput, StdAccountReadOutput {
    identity?: string
    key?: Key
    attributes: Attributes

    constructor(policyName: string) {
        this.identity = policyName
        this.key = SimpleKey(policyName)
        this.attributes = {
            policyName,
            policyQuery: '',
            leftHandEntitlementCount: 0,
            leftHandTotalCount: 0,
            rightHandEntitlementCount: 0,
            rightHandTotalCount: 0,
            totalCount: 0,
            campaignTemplateName: '',
            certificationName: '',
            policyDeleted: false,
            policyConfigured: false,
            policyScheduleConfigured: false,
            campaignDeleted: false,
            campaignConfigured: false,
            campaignScheduleConfigured: false,
            errorMessages: '',
            leftHandEntitlements: '',
            leftHandAccessProfiles: '',
            leftHandRoles: '',
            rightHandEntitlements: '',
            rightHandAccessProfiles: '',
            rightHandRoles: '',
        }
    }
}
