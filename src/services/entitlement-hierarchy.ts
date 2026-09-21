import { logger } from '@sailpoint/connector-sdk'
import {
    Configuration,
    EntitlementsApi,
    Paginator,
    SourcesApi,
} from '../types/sailpoint-api'
import { EntitlementHierarchy } from '../types/enums'
import { EntitlementDocument } from '../types/search-documents'
import { buildIdArray, SearchItemsResult, wrapApiCallResult } from '../utils/api-helper'
import { SearchService } from './search-service'

interface HierarchyIdsResult {
    ids: string[]
    error?: string
}

interface HierarchyDirectionResult {
    direction: EntitlementHierarchy
    error?: string
}

export class EntitlementHierarchyService {
    private readonly hierarchyCache: Record<string, EntitlementHierarchy> = {}

    constructor(private readonly searchService: SearchService) {}

    async includeEntitlementHierarchy(
        apiConfig: Configuration,
        entitlements: EntitlementDocument[]
    ): Promise<SearchItemsResult<EntitlementDocument>> {
        const allHierarchy = await Promise.all(
            entitlements.map((entitlement) => this.getEntitlementHierarchy(apiConfig, entitlement))
        )
        const firstError = allHierarchy.find((result) => result.error)?.error
        if (firstError) {
            return { items: [], error: firstError }
        }
        return {
            items: [...new Map(allHierarchy.flatMap((result) => result.items).map((item) => [item.id, item])).values()],
        }
    }

    async getEntitlementHierarchy(
        apiConfig: Configuration,
        entitlement: EntitlementDocument
    ): Promise<SearchItemsResult<EntitlementDocument>> {
        const directionResult = await this.getEntitlementHierarchyDirection(apiConfig, entitlement)
        if (directionResult.error) {
            return { items: [], error: directionResult.error }
        }

        let entitlementIdsResult: HierarchyIdsResult = { ids: [] }
        if (directionResult.direction === EntitlementHierarchy.CHILD) {
            entitlementIdsResult = await this.getChildEntitlementIds(apiConfig, entitlement.id)
        }
        if (directionResult.direction === EntitlementHierarchy.PARENT) {
            entitlementIdsResult = await this.getParentEntitlementIds(apiConfig, entitlement.id)
        }
        if (entitlementIdsResult.error) {
            return { items: [], error: entitlementIdsResult.error }
        }

        logger.debug(
            `Found ${entitlementIdsResult.ids.length} ${directionResult.direction} entitlements in the hierarchy for entitlement: {${entitlement.id}:${entitlement.name}}`
        )

        if (entitlementIdsResult.ids.length === 0) {
            return { items: [entitlement] }
        }

        const nested = await this.searchService.searchEntitlementsByIds(apiConfig, entitlementIdsResult.ids)
        if (nested.error) {
            return { items: [], error: nested.error }
        }
        return { items: [entitlement, ...nested.items] }
    }

    private async getEntitlementHierarchyDirection(
        apiConfig: Configuration,
        entitlement: EntitlementDocument
    ): Promise<HierarchyDirectionResult> {
        const key = this.getEntitlementSchemaKey(entitlement)
        if (!key) {
            return { direction: EntitlementHierarchy.NONE }
        }
        if (this.hierarchyCache[key]) {
            return { direction: this.hierarchyCache[key] }
        }

        const hierarchyDirection = await this.fetchEntitlementHierarchyDirection(apiConfig, entitlement)
        if (hierarchyDirection.error) {
            return hierarchyDirection
        }
        this.hierarchyCache[key] = hierarchyDirection.direction
        return hierarchyDirection
    }

    private getEntitlementSchemaKey(entitlement: EntitlementDocument): string | undefined {
        if (!entitlement.source?.id || !entitlement.schema) {
            return undefined
        }
        return `${entitlement.source.id}:${entitlement.schema}`
    }

    private async fetchEntitlementHierarchyDirection(
        apiConfig: Configuration,
        entitlement: EntitlementDocument
    ): Promise<HierarchyDirectionResult> {
        if (!entitlement.source) {
            return { direction: EntitlementHierarchy.NONE }
        }

        const sourceApi = new SourcesApi(apiConfig)
        const getSchemasRequest = { sourceId: entitlement.source.id ?? 'N/A' }

        const schemas = await wrapApiCallResult(
            () => sourceApi.getSourceSchemasV1(getSchemasRequest).then((r) => r.data),
            'Error getting source schemas using Sources API',
            getSchemasRequest
        )

        if (!schemas.ok) {
            return { direction: EntitlementHierarchy.NONE, error: schemas.error }
        }
        if (schemas.data.length === 0) {
            return { direction: EntitlementHierarchy.NONE }
        }

        const schema = schemas.data.find((s) => s.name?.toLowerCase() === entitlement.schema?.toLowerCase())
        if (!schema?.hierarchyAttribute) {
            return { direction: EntitlementHierarchy.NONE }
        }

        const childHierarchy = (schema.configuration as { childHierarchy?: boolean | string })?.childHierarchy
        if (childHierarchy === true || childHierarchy === 'true' || childHierarchy === 'True') {
            return { direction: EntitlementHierarchy.PARENT }
        }
        return { direction: EntitlementHierarchy.CHILD }
    }

    private async getChildEntitlementIds(apiConfig: Configuration, entitlementId: string | undefined): Promise<HierarchyIdsResult> {
        if (!entitlementId) {
            return { ids: [] }
        }

        const entitlementsApi = new EntitlementsApi(apiConfig)
        const request = { id: entitlementId }
        const childEntitlements = await wrapApiCallResult(
            () => Paginator.paginate(entitlementsApi, entitlementsApi.listEntitlementChildrenV1, request),
            'Error getting child entitlements using Entitlements API',
            request
        )

        if (!childEntitlements.ok) {
            return { ids: [], error: childEntitlements.error }
        }
        if (childEntitlements.data.data.length === 0) {
            return { ids: [] }
        }

        const nested = await Promise.all(
            childEntitlements.data.data.map((child) => this.getChildEntitlementIds(apiConfig, child.id))
        )
        const nestedError = nested.find((result) => result.error)?.error
        if (nestedError) {
            return { ids: [], error: nestedError }
        }
        return { ids: [...new Set([...nested.flatMap((result) => result.ids), ...buildIdArray(childEntitlements.data.data)])] }
    }

    private async getParentEntitlementIds(apiConfig: Configuration, entitlementId: string | undefined): Promise<HierarchyIdsResult> {
        if (!entitlementId) {
            return { ids: [] }
        }

        const entitlementsApi = new EntitlementsApi(apiConfig)
        const request = { id: entitlementId }
        const parentEntitlements = await wrapApiCallResult(
            () => Paginator.paginate(entitlementsApi, entitlementsApi.listEntitlementParentsV1, request),
            'Error getting parent entitlements using Entitlements API',
            request
        )

        if (!parentEntitlements.ok) {
            return { ids: [], error: parentEntitlements.error }
        }
        if (parentEntitlements.data.data.length === 0) {
            return { ids: [] }
        }

        const nested = await Promise.all(
            parentEntitlements.data.data.map((parent) => this.getParentEntitlementIds(apiConfig, parent.id))
        )
        const nestedError = nested.find((result) => result.error)?.error
        if (nestedError) {
            return { ids: [], error: nestedError }
        }
        return { ids: [...new Set([...nested.flatMap((result) => result.ids), ...buildIdArray(parentEntitlements.data.data)])] }
    }
}
