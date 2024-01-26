using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.Members)]
internal class CronTrigger : Trigger
{
    public string? CronExpression { get; set; }

    /// <summary>
    /// time_zone_id
    /// </summary>
    public required string TimeZone { get; set; }


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
