namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class PausedTriggerGroupId
{
    public string InstanceName { get; set; }

    public string Group { get; set; }


    public PausedTriggerGroupId()
    {
    }

    public PausedTriggerGroupId(string group, string instanceName)
    {
        InstanceName = instanceName;
        Group = group;
    }
}
