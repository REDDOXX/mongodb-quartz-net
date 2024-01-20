namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class FiredTriggerId
{
    public string InstanceName { get; set; }

    public string FiredInstanceId { get; set; }


    public FiredTriggerId()
    {
    }

    public FiredTriggerId(string firedInstanceId, string instanceName)
    {
        InstanceName = instanceName;
        FiredInstanceId = firedInstanceId;
    }
}
