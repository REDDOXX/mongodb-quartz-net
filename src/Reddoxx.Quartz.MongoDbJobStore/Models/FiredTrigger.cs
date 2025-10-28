using System.Globalization;

using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class FiredTrigger
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// </summary>
    /// <remarks>Also called sched_name</remarks>
    public string InstanceName { get; init; }

    /// <summary>
    /// </summary>
    /// <remarks>Also called entry_id</remarks>
    public string FiredInstanceId { get; init; }


    /// <summary>
    /// trigger_name, trigger_group
    /// </summary>
    public TriggerKey TriggerKey { get; init; }

    /// <summary>
    /// job_name, job_group
    /// </summary>
    public JobKey? JobKey { get; init; }

    /// <summary>
    /// instance_name
    /// </summary>
    public string InstanceId { get; init; }

    /// <summary>
    /// fired_time
    /// </summary>
    public DateTimeOffset Fired { get; init; }

    /// <summary>
    /// sched_time
    /// </summary>
    public DateTimeOffset? Scheduled { get; init; }

    /// <summary>
    /// priority
    /// </summary>
    public int Priority { get; init; }

    /// <summary>
    /// state
    /// </summary>
    public LocalTriggerState State { get; init; }

    /// <summary>
    /// is_nonconcurrent
    /// </summary>
    public bool ConcurrentExecutionDisallowed { get; init; }

    /// <summary>
    /// requests_recovery
    /// </summary>
    public bool RequestsRecovery { get; init; }


    public FiredTrigger(
        ObjectId id,
        string instanceName,
        string firedInstanceId,
        TriggerKey triggerKey,
        JobKey? jobKey,
        string instanceId,
        DateTimeOffset fired,
        DateTimeOffset? scheduled,
        int priority,
        LocalTriggerState state,
        bool concurrentExecutionDisallowed,
        bool requestsRecovery
    )
    {
        Id = id;
        InstanceName = instanceName;
        FiredInstanceId = firedInstanceId;
        TriggerKey = triggerKey;
        JobKey = jobKey;
        InstanceId = instanceId;
        Fired = fired;
        Scheduled = scheduled;
        Priority = priority;
        State = state;
        ConcurrentExecutionDisallowed = concurrentExecutionDisallowed;
        RequestsRecovery = requestsRecovery;
    }

    public FiredTrigger(
        string firedInstanceId,
        Trigger trigger,
        JobDetail? jobDetail,
        string instanceId,
        LocalTriggerState state
    )
    {
        Id = ObjectId.GenerateNewId();
        InstanceName = trigger.InstanceName;
        FiredInstanceId = firedInstanceId;
        InstanceId = instanceId;

        TriggerKey = trigger.GetTriggerKey();
        Fired = DateTimeOffset.UtcNow;
        Scheduled = trigger.NextFireTime;
        Priority = trigger.Priority;
        State = state;

        if (jobDetail != null)
        {
            JobKey = jobDetail.GetJobKey();
            ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
            RequestsRecovery = jobDetail.RequestsRecovery;
        }
    }

    public IOperableTrigger GetRecoveryTrigger(JobDataMap jobDataMap)
    {
        var scheduledTime = Scheduled ?? DateTimeOffset.MinValue;

        var name = $"recover_{InstanceId}_{Guid.NewGuid()}";

        var recoveryTrigger = new SimpleTriggerImpl(name, SchedulerConstants.DefaultRecoveryGroup, scheduledTime)
        {
            JobKey = JobKey!,
            Priority = Priority,
            MisfireInstruction = MisfireInstruction.IgnoreMisfirePolicy,
            JobDataMap = jobDataMap,
        };

        recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerName, TriggerKey.Name);
        recoveryTrigger.JobDataMap.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, TriggerKey.Group);
        recoveryTrigger.JobDataMap.Put(
            SchedulerConstants.FailedJobOriginalTriggerFiretime,
            Convert.ToString(Fired, CultureInfo.InvariantCulture)
        );
        recoveryTrigger.JobDataMap.Put(
            SchedulerConstants.FailedJobOriginalTriggerScheduledFiretime,
            Convert.ToString(scheduledTime, CultureInfo.InvariantCulture)
        );

        return recoveryTrigger;
    }
}
