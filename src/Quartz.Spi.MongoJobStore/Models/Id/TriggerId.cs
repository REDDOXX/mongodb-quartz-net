namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class TriggerId
{
    public string InstanceName { get; set; }

    public string Name { get; set; }

    public string Group { get; set; }


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
