using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class SimpleTrigger : Trigger
{
    public int RepeatCount { get; init; }

    public TimeSpan RepeatInterval { get; init; }

    public int TimesTriggered { get; init; }


    public SimpleTrigger(
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
        TimeSpan repeatInterval,
        int timesTriggered
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
        RepeatInterval = repeatInterval;
        TimesTriggered = timesTriggered;
    }

    public SimpleTrigger(ISimpleTrigger trigger, LocalTriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        RepeatCount = trigger.RepeatCount;
        RepeatInterval = trigger.RepeatInterval;
        TimesTriggered = trigger.TimesTriggered;
    }


    public override IOperableTrigger GetTrigger()
    {
        var trigger = new SimpleTriggerImpl
        {
            RepeatCount = RepeatCount,
            RepeatInterval = RepeatInterval,
            TimesTriggered = TimesTriggered,
        };

        FillTrigger(trigger);

        return trigger;
    }
}
