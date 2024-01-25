using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson.Serialization.Attributes;

using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Models;

/*
 INSERT INTO
    {0}JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA)
    VALUES(@schedulerName, @jobName, @jobGroup, @jobDescription, @jobType, @jobDurable, @jobVolatile, @jobStateful, @jobRequestsRecovery, @jobDataMap)
 */

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class JobDetail
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


    [BsonIgnoreIfNull]
    public string? Description { get; set; }

    /// <summary>
    /// job_class_name
    /// </summary>
    public Type JobType { get; set; }

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
    [BsonIgnoreIfNull] // TODO: Serialize to dictionary? 
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

        return JobBuilder.Create(JobType)
            .WithIdentity(GetJobKey())
            .WithDescription(Description)
            .SetJobData(JobDataMap)
            .StoreDurably(Durable)
            .RequestRecovery(RequestsRecovery)
            .Build();
    }

    public JobKey GetJobKey()
    {
        return new JobKey(Name, Group);
    }
}
