using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class FiredTriggerId
{
    public required string InstanceName { get; set; }

    public required string FiredInstanceId { get; set; }


    public FiredTriggerId()
    {
    }

    public FiredTriggerId(string firedInstanceId, string instanceName)
    {
        InstanceName = instanceName;
        FiredInstanceId = firedInstanceId;
    }
}
