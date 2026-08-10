import { SodPolicyLevelEnum } from '../types/sailpoint-api'
import { DtoType } from '../types/enums'

export const MAX_CO_OWNERS = 10

export type ResolvableOwnerType = DtoType.Identity | DtoType.GovernanceGroup

export interface CoOwnerEntry {
    type: ResolvableOwnerType
    value: string
}

export interface ParseCoOwnerEntriesResult {
    entries: CoOwnerEntry[]
    errors: string[]
}

export interface ParsePolicyLevelResult {
    level?: SodPolicyLevelEnum
    error?: string
}

export function normalizeOwnerType(raw: string): ResolvableOwnerType | undefined {
    const normalized = raw.trim().toUpperCase().replace(/\s+/g, '_')
    if (normalized === 'IDENTITY' || normalized === 'INDIVIDUAL') {
        return DtoType.Identity
    }
    if (normalized === 'GOVERNANCE_GROUP' || normalized === 'GOVERNANCEGROUP') {
        return DtoType.GovernanceGroup
    }
    return undefined
}

export function normalizeViolationOwnerType(raw: string): string {
    const normalized = normalizeOwnerType(raw)
    if (normalized) {
        return normalized
    }
    const upper = raw.trim().toUpperCase()
    if (upper === 'MANAGER') {
        return DtoType.Manager
    }
    return raw
}

export function parseCoOwnerEntries(raw: string): ParseCoOwnerEntriesResult {
    const trimmed = raw.trim()
    if (!trimmed) {
        return { entries: [], errors: [] }
    }

    const entries: CoOwnerEntry[] = []
    const errors: string[] = []
    const segments = trimmed.split('|')

    for (const segment of segments) {
        const part = segment.trim()
        if (!part) {
            continue
        }

        const colonIndex = part.indexOf(':')
        if (colonIndex <= 0 || colonIndex === part.length - 1) {
            errors.push(`Invalid co-owner entry [${part}]. Expected format TYPE:value`)
            continue
        }

        const typeRaw = part.slice(0, colonIndex).trim()
        const value = part.slice(colonIndex + 1).trim()
        const type = normalizeOwnerType(typeRaw)

        if (!type) {
            errors.push(`Invalid co-owner type [${typeRaw}] in entry [${part}]`)
            continue
        }
        if (!value) {
            errors.push(`Co-owner value is required in entry [${part}]`)
            continue
        }

        entries.push({ type, value })
    }

    if (entries.length > MAX_CO_OWNERS) {
        errors.push(`CoOwners exceeds maximum of ${MAX_CO_OWNERS} entries`)
    }

    return { entries: entries.slice(0, MAX_CO_OWNERS), errors }
}

export function parsePolicyLevel(raw: string | undefined): ParsePolicyLevelResult {
    if (!raw || !raw.trim()) {
        return { level: SodPolicyLevelEnum.High }
    }

    const normalized = raw.trim().toUpperCase()
    const levelValues = Object.values(SodPolicyLevelEnum) as string[]
    if (levelValues.includes(normalized)) {
        return { level: normalized as SodPolicyLevelEnum }
    }

    return { error: `Invalid Level [${raw}]. Expected one of: ${levelValues.join(', ')}` }
}
