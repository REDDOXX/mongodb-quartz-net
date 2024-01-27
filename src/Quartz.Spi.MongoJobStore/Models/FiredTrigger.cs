using System.Diagnostics.CodeAnalysis;
using System.Globalization;

using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.Members)]
internal class FiredTrigger
{
    [BsonId]
    public ObjectId Id { get; set; }

    /// <summary>
    /// </summary>
    /// <remarks>Also called sched_name</remarks>
    public required string InstanceName { get; set; }

    /// <summary>
    /// </summary>
    /// <remarks>Also called entry_id</remarks>
    public required string FiredInstanceId { get; set; }


    /// <summary>
    /// trigger_name, trigger_group
    /// </summary>
    public required TriggerKey TriggerKey { get; set; }

    /// <summary>
    /// job_name, job_group
    /// </summary>
    public JobKey JobKey { get; set; }

    /// <summary>
    /// instance_name
    /// </summary>
    public required string InstanceId { get; set; }

    /// <summary>
    /// fired_time
    /// </summary>
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime Fired { get; set; }

    /// <summary>
    /// sched_time
    /// </summary>
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? Scheduled { get; set; }

    /// <summary>
    /// priority
    /// </summary>
    public int Priority { get; set; }

    /// <summary>
    /// state
    /// </summary>
    [BsonRepresentation(BsonType.String)]
    public required TriggerState State { get; set; }

    /// <summary>
    /// is_nonconcurrent
    /// </summary>
    public bool ConcurrentExecutionDisallowed { get; set; }

    /// <summary>
    /// requests_recovery
    /// </summary>
    public bool RequestsRecovery { get; set; }


    public FiredTrigger()
    {
    }

    [SetsRequiredMembers]
    public FiredTrigger(string firedInstanceId, Trigger trigger, JobDetail? jobDetail)
    {
        InstanceName = trigger.InstanceName;
        FiredInstanceId = firedInstanceId;

        TriggerKey = trigger.GetTriggerKey();
        Fired = DateTime.UtcNow;
        Scheduled = trigger.NextFireTime;
        Priority = trigger.Priority;
        State = trigger.State;

        if (jobDetail != null)
        {
            JobKey = jobDetail.GetJobKey();
            ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
            RequestsRecovery = jobDetail.RequestsRecovery;
        }
    }

    public IOperableTrigger GetRecoveryTrigger(JobDataMap jobDataMap)
    {
        var firedTime = new DateTimeOffset(Fired);
        var scheduledTime = Scheduled.HasValue ? new DateTimeOffset(Scheduled.Value) : DateTimeOffset.MinValue;

        var name = $"recover_{InstanceId}_{Guid.NewGuid()}";

        var recoveryTrigger = new SimpleTriggerImpl(name, SchedulerConstants.DefaultRecoveryGroup, scheduledTime)
        {
            JobName = JobKey.Name,
            JobGroup = JobKey.Group,
            Priority = Priority,
            MisfireInstruction = MisfireInstruction.IgnoreMisfirePolicy,
            JobDataMap = jobDataMap,
        };

        recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerName, TriggerKey.Name);
        recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, TriggerKey.Group);
        recoveryTrigger.JobDataMap.Put(
            SchedulerConstants.FailedJobOriginalTriggerFiretime,
            Convert.ToString(firedTime, CultureInfo.InvariantCulture)
        );
        recoveryTrigger.JobDataMap.Put(
            SchedulerConstants.FailedJobOriginalTriggerScheduledFiretime,
            Convert.ToString(scheduledTime, CultureInfo.InvariantCulture)
        );

        return recoveryTrigger;
    }
}
