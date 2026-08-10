import { SodPolicyLevelEnum } from './types/sailpoint-api'
import { DtoType } from './types/enums'
import {
    MAX_CO_OWNERS,
    normalizeOwnerType,
    normalizeViolationOwnerType,
    parseCoOwnerEntries,
    parsePolicyLevel,
} from './utils/owner-parser'

describe('owner parser', () => {
    describe('normalizeOwnerType', () => {
        it('accepts identity aliases', () => {
            expect(normalizeOwnerType('IDENTITY')).toBe(DtoType.Identity)
            expect(normalizeOwnerType('Individual')).toBe(DtoType.Identity)
            expect(normalizeOwnerType('INDIVIDUAL')).toBe(DtoType.Identity)
        })

        it('accepts governance group aliases', () => {
            expect(normalizeOwnerType('GOVERNANCE_GROUP')).toBe(DtoType.GovernanceGroup)
            expect(normalizeOwnerType('GovernanceGroup')).toBe(DtoType.GovernanceGroup)
            expect(normalizeOwnerType('Governance Group')).toBe(DtoType.GovernanceGroup)
        })

        it('returns undefined for invalid types', () => {
            expect(normalizeOwnerType('MANAGER')).toBeUndefined()
            expect(normalizeOwnerType('')).toBeUndefined()
        })
    })

    describe('normalizeViolationOwnerType', () => {
        it('preserves manager type', () => {
            expect(normalizeViolationOwnerType('MANAGER')).toBe(DtoType.Manager)
        })
    })

    describe('parseCoOwnerEntries', () => {
        it('parses single and multiple entries', () => {
            const single = parseCoOwnerEntries('IDENTITY:mostafa.helmy')
            expect(single.entries).toEqual([{ type: DtoType.Identity, value: 'mostafa.helmy' }])
            expect(single.errors).toEqual([])

            const multiple = parseCoOwnerEntries('IDENTITY:jane.doe|GOVERNANCE_GROUP:Accounting')
            expect(multiple.entries).toHaveLength(2)
            expect(multiple.errors).toEqual([])
        })

        it('returns empty result for blank input', () => {
            expect(parseCoOwnerEntries('')).toEqual({ entries: [], errors: [] })
        })

        it('reports malformed segments', () => {
            const result = parseCoOwnerEntries('INVALID')
            expect(result.entries).toEqual([])
            expect(result.errors.length).toBeGreaterThan(0)
        })

        it('reports when max co-owners exceeded', () => {
            const entries = Array.from({ length: MAX_CO_OWNERS + 1 }, (_, i) => `IDENTITY:user${i}`).join('|')
            const result = parseCoOwnerEntries(entries)
            expect(result.errors.some((e: string) => e.includes(String(MAX_CO_OWNERS)))).toBe(true)
            expect(result.entries).toHaveLength(MAX_CO_OWNERS)
        })
    })

    describe('parsePolicyLevel', () => {
        it('defaults to HIGH when omitted', () => {
            expect(parsePolicyLevel(undefined).level).toBe(SodPolicyLevelEnum.High)
            expect(parsePolicyLevel('').level).toBe(SodPolicyLevelEnum.High)
        })

        it('accepts valid levels', () => {
            expect(parsePolicyLevel('CRITICAL').level).toBe(SodPolicyLevelEnum.Critical)
            expect(parsePolicyLevel('low').level).toBe(SodPolicyLevelEnum.Low)
        })

        it('returns error for invalid level', () => {
            expect(parsePolicyLevel('URGENT').error).toContain('Invalid Level')
        })
    })
})
