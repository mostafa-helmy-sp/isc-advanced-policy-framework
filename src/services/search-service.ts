import { Configuration, Paginator, Search, SearchApi, SearchIndex } from '../types/sailpoint-api'
import { DtoType } from '../types/enums'
import {
    AccessProfileDocument,
    EntitlementDocument,
    IdentityDocument,
    OwnerReference,
    RoleDocument,
} from '../types/search-documents'
import { buildIdQueryFromItems, wrapApiCall } from '../utils/api-helper'

export class SearchService {
    async searchEntitlementsByQuery(apiConfig: Configuration, query: string): Promise<EntitlementDocument[]> {
        const search = this.buildSearch(SearchIndex.Entitlements, query, [
            'id',
            'name',
            'schema',
            'type',
            'source.name',
            'source.id',
        ])
        const result = await this.runSearch<EntitlementDocument>(apiConfig, search, 'Error finding entitlements using Search API')
        return result ?? []
    }

    async searchAccessProfilesByEntitlements(
        apiConfig: Configuration,
        entitlements: EntitlementDocument[]
    ): Promise<AccessProfileDocument[]> {
        if (entitlements.length === 0) {
            return []
        }
        const query = buildIdQueryFromItems(entitlements, 'id:', ' OR ', '@entitlements(', ')')
        const search = this.buildSearch(SearchIndex.Accessprofiles, query, ['id', 'name', 'type', 'source.name'])
        const result = await this.runSearch<AccessProfileDocument>(
            apiConfig,
            search,
            'Error finding access profiles using Search API'
        )
        return result ?? []
    }

    async searchRolesByAccessProfilesOrEntitlements(
        apiConfig: Configuration,
        entitlements: EntitlementDocument[],
        accessProfiles: AccessProfileDocument[]
    ): Promise<RoleDocument[]> {
        let query: string | undefined
        if (entitlements.length > 0) {
            query = buildIdQueryFromItems(entitlements, 'id:', ' OR ', '@entitlements(', ')')
        }
        if (accessProfiles.length > 0) {
            const accessProfileQuery = buildIdQueryFromItems(accessProfiles, 'accessProfiles.id:', ' OR ')
            query = query ? query + buildIdQueryFromItems(accessProfiles, 'accessProfiles.id:', ' OR ', ' OR ') : accessProfileQuery
        }
        if (!query) {
            return []
        }

        const search = this.buildSearch(SearchIndex.Roles, query, ['id', 'name', 'type'])
        const result = await this.runSearch<RoleDocument>(apiConfig, search, 'Error finding roles using Search API')
        return result ?? []
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

    private buildSearch(index: (typeof SearchIndex)[keyof typeof SearchIndex], query: string, includes: string[]): Search {
        return {
            indices: [index],
            query: { query },
            queryResultFilter: { includes },
            sort: ['id'],
        }
    }

    private async runSearch<T>(apiConfig: Configuration, search: Search, errorContext: string): Promise<T[] | undefined> {
        const searchApi = new SearchApi(apiConfig)
        return wrapApiCall(async () => {
            const response = await Paginator.paginateSearchApi(searchApi, search)
            return response.data as T[]
        }, errorContext, search)
    }
}
