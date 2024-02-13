using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class JobDetail
{
    [BsonId]
    public ObjectId Id { get; set; }

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


    [BsonIgnoreIfNull]
    public string? Description { get; set; }

    /// <summary>
    /// job_class_name
    /// </summary>
    public required Type JobType { get; set; }

    /// <summary>
    /// is_durable
    /// </summary>
    public bool Durable { get; set; }

    /// <summary>
    /// is_nonconcurrent (legacy: jobVolatile)
    /// </summary>
    public bool ConcurrentExecutionDisallowed { get; set; }

    /// <summary>
    /// job_data
    /// </summary>
    public JobDataMap? JobDataMap { get; set; }

    /// <summary>
    /// IS_UPDATE_DATA (legacy: jobStateful)
    /// </summary>
    public bool PersistJobDataAfterExecution { get; set; }

    /// <summary>
    /// requests_recovery
    /// </summary>
    public bool RequestsRecovery { get; set; }


    public JobDetail()
    {
    }

    [SetsRequiredMembers]
    public JobDetail(IJobDetail jobDetail, string instanceName)
    {
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
