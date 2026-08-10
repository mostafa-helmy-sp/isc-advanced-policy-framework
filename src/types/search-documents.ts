/** Minimal shape of an entitlement document returned by the Search API. */
export interface EntitlementDocument {
    id?: string
    name?: string
    schema?: string
    type?: string
    source?: { id?: string; name?: string }
}

/** Minimal shape of an access profile document returned by the Search API. */
export interface AccessProfileDocument {
    id?: string
    name?: string
    type?: string
    source?: { name?: string }
}

/** Minimal shape of a role document returned by the Search API. */
export interface RoleDocument {
    id?: string
    name?: string
    type?: string
}

/** Minimal shape of an identity document returned by the Search API. */
export interface IdentityDocument {
    id?: string
    name?: string
    type?: string
}

/** Owner or recipient reference returned by search / governance group lookups. */
export interface OwnerReference {
    id?: string
    name?: string
    type?: string
}
