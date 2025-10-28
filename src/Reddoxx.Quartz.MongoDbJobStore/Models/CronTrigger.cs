using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class CronTrigger : Trigger
{
    public string? CronExpression { get; init; }

    /// <summary>
    /// time_zone_id
    /// </summary>
    public string TimeZone { get; init; }


    public CronTrigger(
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
        string? cronExpression,
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
        CronExpression = cronExpression;
        TimeZone = timeZone;
    }

    public CronTrigger(ICronTrigger trigger, LocalTriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        CronExpression = trigger.CronExpressionString;
        TimeZone = trigger.TimeZone.Id;
    }

    public override IOperableTrigger GetTrigger()
    {
        var trigger = new CronTriggerImpl
        {
            CronExpressionString = CronExpression,
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
        };

        FillTrigger(trigger);

        return trigger;
    }
}
