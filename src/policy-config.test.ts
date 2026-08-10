import { parsePolicyEnabled, PolicyConfig } from '../src/model/policy-config'
import { buildCampaignAccessConstraints, buildPolicyConflictingAccessCriteria } from '../src/builders/access-constraint-builder'
import { buildCampaignSchedule, buildPolicySchedule } from '../src/builders/schedule-builder'
import { resolveConnectorSettings } from '../src/config/connector-config'
import { DEFAULT_MAX_ENTITLEMENTS_PER_POLICY_SIDE, DEFAULT_WEEKLY_SCHEDULE_DAY } from '../src/config/defaults'

describe('PolicyConfig', () => {
    it('parses enabled values', () => {
        expect(parsePolicyEnabled('true')).toBe(true)
        expect(parsePolicyEnabled('TRUE')).toBe(true)
        expect(parsePolicyEnabled('yes')).toBe(true)
        expect(parsePolicyEnabled('false')).toBe(false)
        expect(parsePolicyEnabled(undefined)).toBe(false)
    })

    it('maps CSV attributes to policy fields', () => {
        const config = new PolicyConfig({
            attributes: {
                PolicyName: 'Test Policy',
                PolicyType: 'SOD',
                PolicyOwnerType: 'IDENTITY',
                PolicyOwner: 'owner',
                PolicyEnabled: 'Yes',
                Query1Name: 'Left',
                Query1: 'tags:LEFT',
                Query2Name: 'Right',
                Query2: 'tags:RIGHT',
                ViolationOwnerType: 'MANAGER',
                ViolationOwner: '',
                MitigatingControls: 'controls',
                CorrectionAdvice: 'advice',
                Actions: 'REPORT,CERTIFY',
                PolicySchedule: 'WEEKLY',
                CertificationName: 'Campaign',
                CertificationDescription: 'Desc',
                CertificationSchedule: 'MONTHLY',
            },
        })

        expect(config.policyName).toBe('Test Policy')
        expect(config.policyState).toBe(true)
        expect(config.actions).toEqual(['REPORT', 'CERTIFY'])
        expect(config.tags).toEqual([])
    })
})

describe('schedule builders', () => {
    const options = {
        hourlyScheduleDay: ['9'],
        weeklyScheduleDay: ['MON', 'TUE'],
        monthlyScheduleDay: ['1', '15'],
    }

    it('builds policy schedules', () => {
        expect(buildPolicySchedule('WEEKLY', options)?.type).toBe('WEEKLY')
        expect(buildPolicySchedule('INVALID', options)).toBeUndefined()
    })

    it('builds campaign schedules with limits', () => {
        const schedule = buildCampaignSchedule('WEEKLY', options)
        expect(schedule?.type).toBe('WEEKLY')
        expect(schedule?.days?.values).toEqual(['MON'])
    })
})

describe('access constraint builder', () => {
    it('builds conflicting access criteria', () => {
        const policyConfig = new PolicyConfig({
            attributes: {
                PolicyName: 'P1',
                PolicyType: 'SOD',
                Query1Name: 'Left',
                Query1: 'q1',
                Query2Name: 'Right',
                Query2: 'q2',
            },
        })

        const criteria = buildPolicyConflictingAccessCriteria(
            policyConfig,
            [{ id: 'e1' }],
            [{ id: 'e2' }]
        )

        expect(criteria.leftCriteria?.criteriaList?.[0].id).toBe('e1')
        expect(criteria.rightCriteria?.criteriaList?.[0].id).toBe('e2')
    })

    it('calculates campaign access metrics', () => {
        const [constraints, leftCount, rightCount, totalCount] = buildCampaignAccessConstraints(
            [{ id: 'e1' }],
            [{ id: 'e2' }],
            [{ id: 'ap1' }],
            [],
            [],
            [{ id: 'r1' }]
        )

        expect(constraints.length).toBeGreaterThan(0)
        expect(leftCount).toBe(2)
        expect(rightCount).toBe(2)
        expect(totalCount).toBe(4)
    })
})

describe('connector defaults', () => {
    it('applies defaults for optional settings', () => {
        const settings = resolveConnectorSettings({
            apiUrl: 'https://tenant.api.identitynow.com',
            clientId: 'id',
            clientSecret: 'secret',
            policyConfigSourceName: 'Policies',
        })

        expect(settings.weeklyScheduleDay).toEqual(DEFAULT_WEEKLY_SCHEDULE_DAY)
        expect(settings.maxEntitlementsPerPolicySide).toBe(DEFAULT_MAX_ENTITLEMENTS_PER_POLICY_SIDE)
        expect(settings.parallelProcessing).toBe(false)
    })
})
