using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models.Id;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class JobDetailId
{
    /// <summary>
    /// schedulerName
    /// </summary>
    public required string InstanceName { get; set; }

    /// <summary>
    /// jobName
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// jobGroup
    /// </summary>
    public required string Group { get; set; }


    public JobDetailId()
    {
    }

    public JobDetailId(JobKey jobKey, string instanceName)
    {
        InstanceName = instanceName;
        Name = jobKey.Name;
        Group = jobKey.Group;
    }

    public JobKey GetJobKey()
    {
        return new JobKey(Name, Group);
    }
}
