using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models.Id;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class SchedulerId
{
    public required string InstanceName { get; set; }

    public required string Id { get; set; }


    public SchedulerId()
    {
    }

    public SchedulerId(string id, string instanceName)
    {
        Id = id;
        InstanceName = instanceName;
    }


    public override string ToString()
    {
        return $"{Id}/{InstanceName}";
    }
}
