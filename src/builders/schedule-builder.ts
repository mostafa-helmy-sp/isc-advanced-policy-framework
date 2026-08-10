import {
    Schedule,
    Schedule2,
    Schedule2DaysTypeEnum,
    Schedule2HoursTypeEnum,
    Schedule2TypeEnum,
    ScheduleType,
    SelectorType,
} from '../types/sailpoint-api'
import {
    MAX_HOURS_PER_CAMPAIGN_SCHEDULE,
    MAX_MONTHLY_DAYS_PER_CAMPAIGN_SCHEDULE,
    MAX_WEEKLY_DAYS_PER_CAMPAIGN_SCHEDULE,
} from '../config/defaults'

export interface ScheduleBuilderOptions {
    hourlyScheduleDay: string[]
    weeklyScheduleDay: string[]
    monthlyScheduleDay: string[]
}

/** Builds a SOD policy report schedule from a schedule type string (DAILY, WEEKLY, MONTHLY). */
export function buildPolicySchedule(scheduleConfig: string, options: ScheduleBuilderOptions): Schedule | undefined {
    if (scheduleConfig === ScheduleType.Daily) {
        return {
            type: ScheduleType.Daily,
            hours: { type: SelectorType.List, values: options.hourlyScheduleDay },
        }
    }
    if (scheduleConfig === ScheduleType.Weekly) {
        return {
            type: ScheduleType.Weekly,
            hours: { type: SelectorType.List, values: options.hourlyScheduleDay },
            days: { type: SelectorType.List, values: options.weeklyScheduleDay },
        }
    }
    if (scheduleConfig === ScheduleType.Monthly) {
        return {
            type: ScheduleType.Monthly,
            hours: { type: SelectorType.List, values: options.hourlyScheduleDay },
            days: { type: SelectorType.List, values: options.monthlyScheduleDay },
        }
    }
    return undefined
}

/** Builds a certification campaign schedule. Campaign schedules support WEEKLY and MONTHLY only. */
export function buildCampaignSchedule(scheduleConfig: string, options: ScheduleBuilderOptions): Schedule2 | undefined {
    const hours = {
        type: Schedule2HoursTypeEnum.List,
        values: options.hourlyScheduleDay.slice(0, MAX_HOURS_PER_CAMPAIGN_SCHEDULE),
    }

    if (scheduleConfig === Schedule2TypeEnum.Weekly) {
        return {
            type: Schedule2TypeEnum.Weekly,
            hours,
            days: {
                type: Schedule2DaysTypeEnum.List,
                values: options.weeklyScheduleDay.slice(0, MAX_WEEKLY_DAYS_PER_CAMPAIGN_SCHEDULE),
            },
        }
    }
    if (scheduleConfig === Schedule2TypeEnum.Monthly) {
        return {
            type: Schedule2TypeEnum.Monthly,
            hours,
            days: {
                type: Schedule2DaysTypeEnum.List,
                values: options.monthlyScheduleDay.slice(0, MAX_MONTHLY_DAYS_PER_CAMPAIGN_SCHEDULE),
            },
        }
    }
    return undefined
}
