using System.Diagnostics.CodeAnalysis;

using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoJobStore.Models;

internal class CronTrigger : Trigger
{
    public string? CronExpression { get; set; }

    /// <summary>
    /// time_zone_id
    /// </summary>
    public string TimeZone { get; set; }


    public CronTrigger()
    {
    }

    [SetsRequiredMembers]
    public CronTrigger(ICronTrigger trigger, TriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        CronExpression = trigger.CronExpressionString;
        TimeZone = trigger.TimeZone.Id;
    }

    public override ITrigger GetTrigger()
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
