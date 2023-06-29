export class PolicyConfig {
    policyName: string
    policyType: string
    policyDescription?: string
    policyOwnerType: string
    policyOwner: string
    policyState: boolean
    externalReference?: string
    tags: string[]
    query1Name: string
    query1: string
    query2Name: string
    query2: string
    violationOwnerType: string
    violationOwner: string
    mitigatingControls: string
    correctionAdvice: string
    actions: string[]
    policySchedule: string
    certificationName: string
    certificationDescription: string
    certificationSchedule: string

    constructor(object: any) {
        this.policyName = object.attributes.PolicyName
        this.policyType = object.attributes.PolicyType
        this.policyDescription = object.attributes.PolicyDescription
        this.policyOwnerType = object.attributes.PolicyOwnerType
        this.policyOwner = object.attributes.PolicyOwner
        if (object.attributes.PolicyEnabled.toLocaleLowerCase() == "true" || object.attributes.PolicyEnabled.toLocaleLowerCase() == "yes") {
            this.policyState = true
        } else {
            this.policyState = false
        }
        this.externalReference = object.attributes.ExternalReference
        if (object.attributes.Tags) {
            this.tags = object.attributes.Tags.split(",")
        } else {
            this.tags = []
        }
        this.query1 = object.attributes.Query1
        this.query2 = object.attributes.Query2
        this.query1Name = object.attributes.Query1Name
        this.query2Name = object.attributes.Query2Name
        this.violationOwnerType = object.attributes.ViolationOwnerType
        this.violationOwner = object.attributes.ViolationOwner
        this.mitigatingControls = object.attributes.MitigatingControls
        this.correctionAdvice = object.attributes.CorrectionAdvice
        if (object.attributes.Tags) {
            this.actions = object.attributes.Actions.split(",")
        } else {
            this.actions = []
        }
        this.policySchedule = object.attributes.PolicySchedule
        this.certificationName = object.attributes.CertificationName
        this.certificationDescription = object.attributes.CertificationDescription
        this.certificationSchedule = object.attributes.CertificationSchedule
    }
}