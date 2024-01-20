namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class SchedulerId
{
    public string InstanceName { get; set; }

    public string Id { get; set; }


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
