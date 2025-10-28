using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class DailyTimeIntervalTrigger : Trigger
{
    public int RepeatCount { get; init; }

    public IntervalUnit RepeatIntervalUnit { get; init; }

    public int RepeatInterval { get; init; }

    public TimeOfDay StartTimeOfDay { get; init; }

    public TimeOfDay? EndTimeOfDay { get; init; }

    public HashSet<DayOfWeek> DaysOfWeek { get; init; }

    public int TimesTriggered { get; init; }

    public string TimeZone { get; init; }


    public DailyTimeIntervalTrigger(
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
        int repeatCount,
        IntervalUnit repeatIntervalUnit,
        int repeatInterval,
        TimeOfDay startTimeOfDay,
        TimeOfDay? endTimeOfDay,
        HashSet<DayOfWeek> daysOfWeek,
        int timesTriggered,
        string timeZone
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
        RepeatCount = repeatCount;
        RepeatIntervalUnit = repeatIntervalUnit;
        RepeatInterval = repeatInterval;
        StartTimeOfDay = startTimeOfDay;
        EndTimeOfDay = endTimeOfDay;
        DaysOfWeek = daysOfWeek;
        TimesTriggered = timesTriggered;
        TimeZone = timeZone;
    }

    public DailyTimeIntervalTrigger(IDailyTimeIntervalTrigger trigger, LocalTriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        RepeatCount = trigger.RepeatCount;
        RepeatIntervalUnit = trigger.RepeatIntervalUnit;
        RepeatInterval = trigger.RepeatInterval;
        StartTimeOfDay = trigger.StartTimeOfDay;
        EndTimeOfDay = trigger.EndTimeOfDay;
        DaysOfWeek = new HashSet<DayOfWeek>(trigger.DaysOfWeek);
        TimesTriggered = trigger.TimesTriggered;
        TimeZone = trigger.TimeZone.Id;
    }

    public override IOperableTrigger GetTrigger()
    {
        var trigger = new DailyTimeIntervalTriggerImpl
        {
            RepeatCount = RepeatCount,
            RepeatIntervalUnit = RepeatIntervalUnit,
            RepeatInterval = RepeatInterval,
            StartTimeOfDay = StartTimeOfDay,
            EndTimeOfDay = EndTimeOfDay ?? new TimeOfDay(23, 59, 59),
            DaysOfWeek = new HashSet<DayOfWeek>(DaysOfWeek),
            TimesTriggered = TimesTriggered,
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
        };

        FillTrigger(trigger);

        return trigger;
    }
}
