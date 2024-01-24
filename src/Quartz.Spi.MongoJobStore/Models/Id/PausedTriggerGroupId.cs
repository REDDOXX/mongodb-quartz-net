using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models.Id;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class PausedTriggerGroupId
{
    public required string InstanceName { get; set; }

    public required string Group { get; set; }


    public PausedTriggerGroupId()
    {
    }

    public PausedTriggerGroupId(string group, string instanceName)
    {
        InstanceName = instanceName;
        Group = group;
    }
}
