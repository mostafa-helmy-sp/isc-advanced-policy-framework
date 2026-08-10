import {
    Configuration,
    GovernanceGroupsApi,
    Paginator,
    SodPolicySecondaryOwnerRefsInner,
} from '../types/sailpoint-api'
import { PolicyConfig } from '../model/policy-config'
import { DtoType } from '../types/enums'
import { OwnerReference } from '../types/search-documents'
import { parseCoOwnerEntries } from '../utils/owner-parser'
import { wrapApiCall } from '../utils/api-helper'
import { SearchService } from './search-service'

export interface ResolveCoOwnersResult {
    refs: SodPolicySecondaryOwnerRefsInner[]
    errors: string[]
}

export class OwnerResolverService {
    constructor(
        private readonly searchService: SearchService,
        private readonly identityResolutionAttribute: string
    ) {}

    async resolvePolicyOwner(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<OwnerReference | undefined> {
        return this.resolveOwnerReference(apiConfig, policyConfig.policyOwnerType, policyConfig.policyOwner)
    }

    async resolveViolationOwner(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<OwnerReference | undefined> {
        if (policyConfig.violationOwnerType === DtoType.Manager) {
            return undefined
        }
        if (!policyConfig.violationOwner) {
            return undefined
        }
        return this.resolveOwnerReference(apiConfig, policyConfig.violationOwnerType, policyConfig.violationOwner)
    }

    async resolveCoOwners(apiConfig: Configuration, coOwnersRaw: string): Promise<ResolveCoOwnersResult> {
        const { entries, errors } = parseCoOwnerEntries(coOwnersRaw)
        const refs: SodPolicySecondaryOwnerRefsInner[] = []

        for (const entry of entries) {
            const resolved = await this.resolveOwnerReference(apiConfig, entry.type, entry.value)
            if (!resolved) {
                errors.push(`Unable to resolve Co-Owner. Type: ${entry.type}, Value: ${entry.value}`)
                continue
            }
            refs.push(resolved as SodPolicySecondaryOwnerRefsInner)
        }

        return { refs, errors }
    }

    async resolvePolicyRecipients(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        violationOwner: OwnerReference | undefined,
        policyOwner: OwnerReference | undefined
    ): Promise<OwnerReference[]> {
        let recipients: OwnerReference[] = []

        if (policyConfig.violationOwnerType === DtoType.Identity && policyConfig.violationOwner && violationOwner) {
            recipients = [violationOwner]
        } else if (
            policyConfig.violationOwnerType === DtoType.GovernanceGroup &&
            policyConfig.violationOwner &&
            violationOwner?.id
        ) {
            recipients = await this.findGovGroupMembers(apiConfig, violationOwner.id)
        }

        if (recipients.length === 0 && policyOwner) {
            recipients = [policyOwner]
        }
        return recipients
    }

    private async resolveOwnerReference(
        apiConfig: Configuration,
        ownerType: string,
        ownerValue: string
    ): Promise<OwnerReference | undefined> {
        if (ownerType === DtoType.Identity) {
            return this.searchService.searchIdentityByAttribute(apiConfig, this.identityResolutionAttribute, ownerValue)
        }
        if (ownerType === DtoType.GovernanceGroup) {
            return this.searchGovGroupByName(apiConfig, ownerValue)
        }
        return undefined
    }

    private async searchGovGroupByName(apiConfig: Configuration, govGroupName: string): Promise<OwnerReference | undefined> {
        const filter = `name eq "${govGroupName}"`
        const govGroupApi = new GovernanceGroupsApi(apiConfig)
        const request = { filters: filter }

        const existingGovGroup = await wrapApiCall(
            () => govGroupApi.listWorkgroupsV1(request).then((r) => r.data),
            'Error finding Governance Group using Governance-Groups API',
            request
        )

        if (!existingGovGroup || existingGovGroup.length === 0) {
            return undefined
        }

        const govGroup = existingGovGroup[0]
        return { id: govGroup.id, name: govGroup.name, type: DtoType.GovernanceGroup }
    }

    private async findGovGroupMembers(apiConfig: Configuration, govGroupId: string): Promise<OwnerReference[]> {
        const govGroupApi = new GovernanceGroupsApi(apiConfig)
        const request = { workgroupId: govGroupId }

        const govGroupMembers = await wrapApiCall(
            () => Paginator.paginate(govGroupApi, govGroupApi.listWorkgroupMembersV1, request),
            'Error finding Governance Group members using Governance-Groups API',
            request
        )

        if (!govGroupMembers || govGroupMembers.data.length === 0) {
            return []
        }

        return govGroupMembers.data.map((member) => ({
            id: member.id,
            type: DtoType.Identity,
            name: member.name,
        }))
    }
}
