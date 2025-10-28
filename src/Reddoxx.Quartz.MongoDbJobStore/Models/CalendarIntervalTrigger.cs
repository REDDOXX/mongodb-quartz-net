using MongoDB.Bson;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

internal class CalendarIntervalTrigger : Trigger
{
    public IntervalUnit RepeatIntervalUnit { get; }

    public int RepeatInterval { get; }

    public int TimesTriggered { get; }

    public string TimeZone { get; }

    public bool PreserveHourOfDayAcrossDaylightSavings { get; }

    public bool SkipDayIfHourDoesNotExist { get; }


    public CalendarIntervalTrigger(
        ObjectId id,
        string instanceName,
        string name,
        string group,
        string? description,
        DateTimeOffset? nextFireTime,
        DateTimeOffset? previousFireTime,
        LocalTriggerState state,
        DateTimeOffset startTime,
        DateTimeOffset? endTime,
        string? calendarName,
        int misfireInstruction,
        int priority,
        JobDataMap jobDataMap,
        JobKey jobKey,
        IntervalUnit repeatIntervalUnit,
        int repeatInterval,
        int timesTriggered,
        string timeZone,
        bool preserveHourOfDayAcrossDaylightSavings,
        bool skipDayIfHourDoesNotExist
    )
        : base(
            id,
            instanceName,
            name,
            group,
            description,
            nextFireTime,
            previousFireTime,
            state,
            startTime,
            endTime,
            calendarName,
            misfireInstruction,
            priority,
            jobDataMap,
            jobKey
        )
    {
        RepeatIntervalUnit = repeatIntervalUnit;
        RepeatInterval = repeatInterval;
        TimesTriggered = timesTriggered;
        TimeZone = timeZone;
        PreserveHourOfDayAcrossDaylightSavings = preserveHourOfDayAcrossDaylightSavings;
        SkipDayIfHourDoesNotExist = skipDayIfHourDoesNotExist;
    }

    public CalendarIntervalTrigger(ICalendarIntervalTrigger trigger, LocalTriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        RepeatIntervalUnit = trigger.RepeatIntervalUnit;
        RepeatInterval = trigger.RepeatInterval;
        TimesTriggered = trigger.TimesTriggered;
        TimeZone = trigger.TimeZone.Id;
        PreserveHourOfDayAcrossDaylightSavings = trigger.PreserveHourOfDayAcrossDaylightSavings;
        SkipDayIfHourDoesNotExist = trigger.SkipDayIfHourDoesNotExist;
    }

    public override IOperableTrigger GetTrigger()
    {
        var trigger = new CalendarIntervalTriggerImpl
        {
            RepeatIntervalUnit = RepeatIntervalUnit,
            RepeatInterval = RepeatInterval,
            TimesTriggered = TimesTriggered,
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
            PreserveHourOfDayAcrossDaylightSavings = PreserveHourOfDayAcrossDaylightSavings,
            SkipDayIfHourDoesNotExist = SkipDayIfHourDoesNotExist,
        };

        FillTrigger(trigger);

        return trigger;
    }
}
