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
import { wrapApiCallResult } from '../utils/api-helper'
import { SearchService } from './search-service'

export interface ResolveOwnerResult {
    owner?: OwnerReference
    error?: string
}

export interface ResolveCoOwnersResult {
    refs: SodPolicySecondaryOwnerRefsInner[]
    errors: string[]
}

export interface ResolveRecipientsResult {
    recipients: OwnerReference[]
    error?: string
}

export class OwnerResolverService {
    private readonly ownerCache = new Map<string, OwnerReference | undefined>()
    private readonly govGroupMembersCache = new Map<string, OwnerReference[]>()

    constructor(
        private readonly searchService: SearchService,
        private readonly identityResolutionAttribute: string
    ) {}

    clearCaches(): void {
        this.ownerCache.clear()
        this.govGroupMembersCache.clear()
    }

    async resolvePolicyOwner(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<ResolveOwnerResult> {
        return this.resolveOwnerReference(apiConfig, policyConfig.policyOwnerType, policyConfig.policyOwner)
    }

    async resolveViolationOwner(apiConfig: Configuration, policyConfig: PolicyConfig): Promise<ResolveOwnerResult> {
        if (policyConfig.violationOwnerType === DtoType.Manager) {
            return {}
        }
        if (!policyConfig.violationOwner) {
            return {}
        }
        return this.resolveOwnerReference(apiConfig, policyConfig.violationOwnerType, policyConfig.violationOwner)
    }

    async resolveCoOwners(apiConfig: Configuration, coOwnersRaw: string): Promise<ResolveCoOwnersResult> {
        const { entries, errors } = parseCoOwnerEntries(coOwnersRaw)
        const resolvedEntries = await Promise.all(
            entries.map(async (entry) => ({
                entry,
                result: await this.resolveOwnerReference(apiConfig, entry.type, entry.value),
            }))
        )

        const refs: SodPolicySecondaryOwnerRefsInner[] = []
        for (const { entry, result } of resolvedEntries) {
            if (result.error) {
                errors.push(result.error)
                continue
            }
            if (!result.owner) {
                errors.push(`Unable to resolve Co-Owner. Type: ${entry.type}, Value: ${entry.value}`)
                continue
            }
            refs.push(result.owner as SodPolicySecondaryOwnerRefsInner)
        }

        return { refs, errors }
    }

    async resolvePolicyRecipients(
        apiConfig: Configuration,
        policyConfig: PolicyConfig,
        violationOwner: OwnerReference | undefined,
        policyOwner: OwnerReference | undefined
    ): Promise<ResolveRecipientsResult> {
        let recipients: OwnerReference[] = []

        if (policyConfig.violationOwnerType === DtoType.Identity && policyConfig.violationOwner && violationOwner) {
            recipients = [violationOwner]
        } else if (
            policyConfig.violationOwnerType === DtoType.GovernanceGroup &&
            policyConfig.violationOwner &&
            violationOwner?.id
        ) {
            const membersResult = await this.findGovGroupMembers(apiConfig, violationOwner.id)
            if (membersResult.error) {
                return { recipients: [], error: membersResult.error }
            }
            recipients = membersResult.members
        }

        if (recipients.length === 0 && policyOwner) {
            recipients = [policyOwner]
        }
        return { recipients }
    }

    private ownerCacheKey(ownerType: string, ownerValue: string): string {
        return `${ownerType}:${ownerValue}`
    }

    private async resolveOwnerReference(
        apiConfig: Configuration,
        ownerType: string,
        ownerValue: string
    ): Promise<ResolveOwnerResult> {
        const cacheKey = this.ownerCacheKey(ownerType, ownerValue)
        if (this.ownerCache.has(cacheKey)) {
            return { owner: this.ownerCache.get(cacheKey) }
        }

        if (ownerType === DtoType.Identity) {
            const result = await this.searchService.searchIdentityByAttribute(
                apiConfig,
                this.identityResolutionAttribute,
                ownerValue
            )
            if (result.error) {
                return { error: result.error }
            }
            this.ownerCache.set(cacheKey, result.identity)
            return { owner: result.identity }
        }

        if (ownerType === DtoType.GovernanceGroup) {
            const result = await this.searchGovGroupByName(apiConfig, ownerValue)
            if (result.error) {
                return { error: result.error }
            }
            this.ownerCache.set(cacheKey, result.owner)
            return { owner: result.owner }
        }

        return {}
    }

    private async searchGovGroupByName(apiConfig: Configuration, govGroupName: string): Promise<ResolveOwnerResult> {
        const filter = `name eq "${govGroupName}"`
        const govGroupApi = new GovernanceGroupsApi(apiConfig)
        const request = { filters: filter }

        const existingGovGroup = await wrapApiCallResult(
            () => govGroupApi.listWorkgroupsV1(request).then((r) => r.data),
            'Error finding Governance Group using Governance-Groups API',
            request
        )

        if (!existingGovGroup.ok) {
            return { error: existingGovGroup.error }
        }
        if (existingGovGroup.data.length === 0) {
            return {}
        }

        const govGroup = existingGovGroup.data[0]
        return { owner: { id: govGroup.id, name: govGroup.name, type: DtoType.GovernanceGroup } }
    }

    private async findGovGroupMembers(
        apiConfig: Configuration,
        govGroupId: string
    ): Promise<{ members: OwnerReference[]; error?: string }> {
        if (this.govGroupMembersCache.has(govGroupId)) {
            return { members: this.govGroupMembersCache.get(govGroupId)! }
        }

        const govGroupApi = new GovernanceGroupsApi(apiConfig)
        const request = { workgroupId: govGroupId }

        const govGroupMembers = await wrapApiCallResult(
            () => Paginator.paginate(govGroupApi, govGroupApi.listWorkgroupMembersV1, request),
            'Error finding Governance Group members using Governance-Groups API',
            request
        )

        if (!govGroupMembers.ok) {
            return { members: [], error: govGroupMembers.error }
        }

        const members =
            govGroupMembers.data.data.length === 0
                ? []
                : govGroupMembers.data.data.map((member) => ({
                      id: member.id,
                      type: DtoType.Identity,
                      name: member.name,
                  }))

        this.govGroupMembersCache.set(govGroupId, members)
        return { members }
    }
}
