import { logger } from '@sailpoint/connector-sdk'
import {
    Configuration,
    EntitlementsApi,
    Paginator,
    SourcesApi,
} from '../types/sailpoint-api'
import { EntitlementHierarchy } from '../types/enums'
import { EntitlementDocument } from '../types/search-documents'
import { buildIdArray, wrapApiCall } from '../utils/api-helper'
import { SearchService } from './search-service'

export class EntitlementHierarchyService {
    private readonly hierarchyCache: Record<string, EntitlementHierarchy> = {}

    constructor(private readonly searchService: SearchService) {}

    async includeEntitlementHierarchy(
        apiConfig: Configuration,
        entitlements: EntitlementDocument[]
    ): Promise<EntitlementDocument[]> {
        const allHierarchy = await Promise.all(
            entitlements.map((entitlement) => this.getEntitlementHierarchy(apiConfig, entitlement))
        )
        return [...new Map(allHierarchy.flat().map((item) => [item.id, item])).values()]
    }

    async getEntitlementHierarchy(
        apiConfig: Configuration,
        entitlement: EntitlementDocument
    ): Promise<EntitlementDocument[]> {
        const hierarchyDirection = await this.getEntitlementHierarchyDirection(apiConfig, entitlement)
        let entitlementIds: string[] = []

        if (hierarchyDirection === EntitlementHierarchy.CHILD) {
            entitlementIds = await this.getChildEntitlementIds(apiConfig, entitlement.id)
        }
        if (hierarchyDirection === EntitlementHierarchy.PARENT) {
            entitlementIds = await this.getParentEntitlementIds(apiConfig, entitlement.id)
        }

        logger.debug(
            `Found ${entitlementIds.length} ${hierarchyDirection} entitlements in the hierarchy for entitlement: {${entitlement.id}:${entitlement.name}}`
        )

        if (entitlementIds.length === 0) {
            return [entitlement]
        }

        const nested = await this.searchService.searchEntitlementsByIds(apiConfig, entitlementIds)
        return [entitlement, ...nested]
    }

    private async getEntitlementHierarchyDirection(
        apiConfig: Configuration,
        entitlement: EntitlementDocument
    ): Promise<EntitlementHierarchy> {
        const key = this.getEntitlementSchemaKey(entitlement)
        if (!key) {
            return EntitlementHierarchy.NONE
        }
        if (this.hierarchyCache[key]) {
            return this.hierarchyCache[key]
        }

        const hierarchyDirection = await this.fetchEntitlementHierarchyDirection(apiConfig, entitlement)
        this.hierarchyCache[key] = hierarchyDirection
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
    ): Promise<EntitlementHierarchy> {
        if (!entitlement.source) {
            return EntitlementHierarchy.NONE
        }

        const sourceApi = new SourcesApi(apiConfig)
        const getSchemasRequest = { sourceId: entitlement.source.id ?? 'N/A' }

        const schemas = await wrapApiCall(
            () => sourceApi.getSourceSchemasV1(getSchemasRequest).then((r) => r.data),
            'Error getting source schemas using Sources API',
            getSchemasRequest
        )

        if (!schemas || schemas.length === 0) {
            return EntitlementHierarchy.NONE
        }

        const schema = schemas.find((s) => s.name?.toLowerCase() === entitlement.schema?.toLowerCase())
        if (!schema?.hierarchyAttribute) {
            return EntitlementHierarchy.NONE
        }

        const childHierarchy = (schema.configuration as { childHierarchy?: boolean | string })?.childHierarchy
        if (childHierarchy === true || childHierarchy === 'true' || childHierarchy === 'True') {
            return EntitlementHierarchy.PARENT
        }
        return EntitlementHierarchy.CHILD
    }

    private async getChildEntitlementIds(apiConfig: Configuration, entitlementId: string | undefined): Promise<string[]> {
        if (!entitlementId) {
            return []
        }

        const entitlementsApi = new EntitlementsApi(apiConfig)
        const request = { id: entitlementId }
        const childEntitlements = await wrapApiCall(
            () => Paginator.paginate(entitlementsApi, entitlementsApi.listEntitlementChildrenV1, request),
            'Error getting child entitlements using Entitlements API',
            request
        )

        if (!childEntitlements || childEntitlements.data.length === 0) {
            return []
        }

        const nested = await Promise.all(
            childEntitlements.data.map((child) => this.getChildEntitlementIds(apiConfig, child.id))
        )
        return [...new Set([...nested.flat(), ...buildIdArray(childEntitlements.data)])]
    }

    private async getParentEntitlementIds(apiConfig: Configuration, entitlementId: string | undefined): Promise<string[]> {
        if (!entitlementId) {
            return []
        }

        const entitlementsApi = new EntitlementsApi(apiConfig)
        const request = { id: entitlementId }
        const parentEntitlements = await wrapApiCall(
            () => Paginator.paginate(entitlementsApi, entitlementsApi.listEntitlementParentsV1, request),
            'Error getting parent entitlements using Entitlements API',
            request
        )

        if (!parentEntitlements || parentEntitlements.data.length === 0) {
            return []
        }

        const nested = await Promise.all(
            parentEntitlements.data.map((parent) => this.getParentEntitlementIds(apiConfig, parent.id))
        )
        return [...new Set([...nested.flat(), ...buildIdArray(parentEntitlements.data)])]
    }
}
