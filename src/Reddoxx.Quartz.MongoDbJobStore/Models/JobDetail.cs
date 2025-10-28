using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class JobDetail
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// schedulerName
    /// </summary>
    public string InstanceName { get; init; }

    /// <summary>
    /// jobName
    /// </summary>
    public string Name { get; init; }

    /// <summary>
    /// jobGroup
    /// </summary>
    public string Group { get; init; }

    public string? Description { get; init; }

    /// <summary>
    /// job_class_name
    /// </summary>
    public Type JobType { get; init; }

    /// <summary>
    /// is_durable
    /// </summary>
    public bool Durable { get; init; }

    /// <summary>
    /// is_nonconcurrent (legacy: jobVolatile)
    /// </summary>
    public bool ConcurrentExecutionDisallowed { get; init; }

    /// <summary>
    /// job_data
    /// </summary>
    public JobDataMap? JobDataMap { get; init; }

    /// <summary>
    /// IS_UPDATE_DATA (legacy: jobStateful)
    /// </summary>
    public bool PersistJobDataAfterExecution { get; init; }

    /// <summary>
    /// requests_recovery
    /// </summary>
    public bool RequestsRecovery { get; init; }


    /// <summary>
    /// MongoDb ctor
    /// </summary>
    public JobDetail(
        ObjectId id,
        string instanceName,
        string name,
        string group,
        string? description,
        Type jobType,
        bool durable,
        bool concurrentExecutionDisallowed,
        JobDataMap? jobDataMap,
        bool persistJobDataAfterExecution,
        bool requestsRecovery
    )
    {
        Id = id;
        InstanceName = instanceName;
        Name = name;
        Group = group;
        Description = description;
        JobType = jobType;
        Durable = durable;
        ConcurrentExecutionDisallowed = concurrentExecutionDisallowed;
        JobDataMap = jobDataMap;
        PersistJobDataAfterExecution = persistJobDataAfterExecution;
        RequestsRecovery = requestsRecovery;
    }

    public JobDetail(IJobDetail jobDetail, string instanceName)
    {
        Id = ObjectId.GenerateNewId();
        InstanceName = instanceName;
        Name = jobDetail.Key.Name;
        Group = jobDetail.Key.Group;

        Description = jobDetail.Description;
        JobType = jobDetail.JobType;
        JobDataMap = jobDetail.JobDataMap;
        Durable = jobDetail.Durable;
        PersistJobDataAfterExecution = jobDetail.PersistJobDataAfterExecution;
        ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
        RequestsRecovery = jobDetail.RequestsRecovery;
    }

    public IJobDetail GetJobDetail()
    {
        // The missing properties are figured out at runtime from the job type attributes
        return JobBuilder.Create()
                         .OfType(JobType)
                         .RequestRecovery(RequestsRecovery)
                         .StoreDurably(Durable)
                         .DisallowConcurrentExecution(ConcurrentExecutionDisallowed)
                         .PersistJobDataAfterExecution(PersistJobDataAfterExecution)
                         .WithDescription(Description)
                         .WithIdentity(GetJobKey())
                         .SetJobData(JobDataMap)
                         .Build();
    }

    public JobKey GetJobKey()
    {
        return new JobKey(Name, Group);
    }
}
