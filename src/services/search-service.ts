import { Configuration, Paginator, Search, SearchApi, SearchIndex } from '../types/sailpoint-api'
import { SEARCH_QUERY_BATCH_SIZE } from '../config/defaults'
import { DtoType } from '../types/enums'
import {
    AccessProfileDocument,
    EntitlementDocument,
    IdentityDocument,
    OwnerReference,
    RoleDocument,
} from '../types/search-documents'
import { buildBatchedIdQueries, buildIdArray, deduplicateById, wrapApiCall } from '../utils/api-helper'

export class SearchService {
    async searchEntitlementsByQuery(apiConfig: Configuration, query: string): Promise<EntitlementDocument[]> {
        const search = this.buildSearch(SearchIndex.Entitlements, query, this.entitlementIncludes())
        const result = await this.runSearch<EntitlementDocument>(apiConfig, search, 'Error finding entitlements using Search API')
        return result ?? []
    }

    async searchEntitlementsByIds(apiConfig: Configuration, ids: string[]): Promise<EntitlementDocument[]> {
        if (ids.length === 0) {
            return []
        }

        const queries = buildBatchedIdQueries(ids, SEARCH_QUERY_BATCH_SIZE, 'id:', ' OR ')
        return this.runBatchedQueries<EntitlementDocument>(
            apiConfig,
            queries,
            SearchIndex.Entitlements,
            this.entitlementIncludes(),
            'Error finding entitlements using Search API'
        )
    }

    async searchAccessProfilesByEntitlements(
        apiConfig: Configuration,
        entitlements: EntitlementDocument[]
    ): Promise<AccessProfileDocument[]> {
        const entitlementIds = buildIdArray(entitlements)
        if (entitlementIds.length === 0) {
            return []
        }

        const queries = buildBatchedIdQueries(
            entitlementIds,
            SEARCH_QUERY_BATCH_SIZE,
            'id:',
            ' OR ',
            '@entitlements(',
            ')'
        )
        return this.runBatchedQueries<AccessProfileDocument>(
            apiConfig,
            queries,
            SearchIndex.Accessprofiles,
            ['id', 'name', 'type', 'source.name'],
            'Error finding access profiles using Search API'
        )
    }

    async searchRolesByAccessProfilesOrEntitlements(
        apiConfig: Configuration,
        entitlements: EntitlementDocument[],
        accessProfiles: AccessProfileDocument[]
    ): Promise<RoleDocument[]> {
        const entitlementIds = buildIdArray(entitlements)
        const accessProfileIds = buildIdArray(accessProfiles)
        const queries: string[] = []

        if (entitlementIds.length > 0) {
            queries.push(
                ...buildBatchedIdQueries(
                    entitlementIds,
                    SEARCH_QUERY_BATCH_SIZE,
                    'id:',
                    ' OR ',
                    '@entitlements(',
                    ')'
                )
            )
        }
        if (accessProfileIds.length > 0) {
            queries.push(...buildBatchedIdQueries(accessProfileIds, SEARCH_QUERY_BATCH_SIZE, 'accessProfiles.id:', ' OR '))
        }
        if (queries.length === 0) {
            return []
        }

        return this.runBatchedQueries<RoleDocument>(
            apiConfig,
            queries,
            SearchIndex.Roles,
            ['id', 'name', 'type'],
            'Error finding roles using Search API'
        )
    }

    async searchIdentityByAttribute(
        apiConfig: Configuration,
        attribute: string,
        value: string
    ): Promise<OwnerReference | undefined> {
        let query: string
        if (attribute === 'name' || attribute === 'employeeNumber' || attribute === 'id') {
            query = `${attribute}.exact:"${value}"`
        } else {
            query = `attributes.${attribute}.exact:"${value}"`
        }

        const search = this.buildSearch(SearchIndex.Identities, query, ['id', 'name', 'type'])
        const identities = await this.runSearch<IdentityDocument>(apiConfig, search, 'Error finding identity using Search API')
        if (!identities || identities.length === 0) {
            return undefined
        }
        const identity = identities[0]
        return { id: identity.id, name: identity.name, type: DtoType.Identity }
    }

    private entitlementIncludes(): string[] {
        return ['id', 'name', 'schema', 'type', 'source.name', 'source.id']
    }

    private buildSearch(index: (typeof SearchIndex)[keyof typeof SearchIndex], query: string, includes: string[]): Search {
        return {
            indices: [index],
            query: { query },
            queryResultFilter: { includes },
            sort: ['id'],
        }
    }

    private async runBatchedQueries<T extends { id?: string }>(
        apiConfig: Configuration,
        queries: string[],
        index: (typeof SearchIndex)[keyof typeof SearchIndex],
        includes: string[],
        errorContext: string
    ): Promise<T[]> {
        const batchResults = await Promise.all(
            queries.map((query) => {
                const search = this.buildSearch(index, query, includes)
                return this.runSearch<T>(apiConfig, search, errorContext)
            })
        )
        return deduplicateById(batchResults.flatMap((result) => result ?? []))
    }

    private async runSearch<T>(apiConfig: Configuration, search: Search, errorContext: string): Promise<T[] | undefined> {
        const searchApi = new SearchApi(apiConfig)
        return wrapApiCall(async () => {
            const response = await Paginator.paginateSearchApi(searchApi, search)
            return response.data as T[]
        }, errorContext, search)
    }
}
