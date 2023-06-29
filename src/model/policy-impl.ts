import { Attributes } from "@sailpoint/connector-sdk"

export class PolicyImpl {
    identity: string
    uuid: string
    attributes: Attributes

    constructor(policyName: string) {
        this.identity = policyName
        this.uuid = policyName
        this.attributes = {
            policyName: policyName,
            policyQuery: "",
            leftHandEntitlementCount: 0,
            leftHandTotalCount: 0,
            rightHandEntitlementCount: 0,
            rightHandTotalCount: 0,
            totalCount: 0,
            campaignTemplateName: "",
            policyDeleted: false,
            policyConfigured: false,
            policyScheduleConfigured: false,
            campaignDeleted: false,
            campaignConfigured: false,
            campaignScheduleConfigured: false,
            errorMessages: "",
            leftHandEntitlements: "",
            leftHandAccessProfiles: "",
            leftHandRoles: "",
            rightHandEntitlements: "",
            rightHandAccessProfiles: "",
            rightHandRoles: ""
        }
    }
}