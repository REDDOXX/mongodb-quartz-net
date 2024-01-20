namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class JobDetailId
{
    /// <summary>
    /// schedulerName
    /// </summary>
    public string InstanceName { get; set; }

    /// <summary>
    /// jobName
    /// </summary>
    public string Name { get; set; }

    /// <summary>
    /// jobGroup
    /// </summary>
    public string Group { get; set; }


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
