namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class FiredTriggerId : BaseId
{
    public FiredTriggerId()
    {
    }

    public FiredTriggerId(string firedInstanceId, string instanceName)
    {
        InstanceName = instanceName;
        FiredInstanceId = firedInstanceId;
    }

    public string FiredInstanceId { get; set; }
}
