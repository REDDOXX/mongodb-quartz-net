using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models.Id;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class TriggerId
{
    public required string InstanceName { get; set; }

    public required string Name { get; set; }

    public required string Group { get; set; }


    public TriggerId()
    {
    }

    public TriggerId(TriggerKey triggerKey, string instanceName)
    {
        InstanceName = instanceName;
        Name = triggerKey.Name;
        Group = triggerKey.Group;
    }

    public TriggerKey GetTriggerKey()
    {
        return new TriggerKey(Name, Group);
    }
}
