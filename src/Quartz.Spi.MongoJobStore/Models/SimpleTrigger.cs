using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class SimpleTrigger : Trigger
{
    public int RepeatCount { get; set; }

    public TimeSpan RepeatInterval { get; set; }

    public int TimesTriggered { get; set; }


    public SimpleTrigger()
    {
    }

    [SetsRequiredMembers]
    public SimpleTrigger(ISimpleTrigger trigger, TriggerState state, string instanceName)
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
