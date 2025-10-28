using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

/// <summary>
/// Database trigger state
/// </summary>
internal enum LocalTriggerState
{
    Waiting,
    Acquired,
    Executing,
    Complete,
    Blocked,
    Error,
    Paused,
    PausedBlocked,
    Deleted,
}

/// <summary>
/// 
/// </summary>
/// <remarks>
/// trigger_type has been removed as we're using the mongodb inheritance feature.
/// </remarks>
[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal abstract class Trigger
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// sched_name
    /// </summary>
    public string InstanceName { get; init; }

    /// <summary>
    /// trigger_name
    /// </summary>
    public string Name { get; init; }

    /// <summary>
    /// trigger_group
    /// </summary>
    public string Group { get; init; }


    /// <summary>
    /// description
    /// </summary>
    public string? Description { get; init; }

    /// <summary>
    /// next_fire_time
    /// </summary>
    public DateTimeOffset? NextFireTime { get; init; }

    /// <summary>
    /// prev_fire_time
    /// </summary>
    public DateTimeOffset? PreviousFireTime { get; init; }

    /// <summary>
    /// trigger_state
    /// </summary>
    public LocalTriggerState State { get; init; }

    /// <summary>
    /// start_time
    /// </summary>
    public DateTimeOffset StartTime { get; init; }

    /// <summary>
    /// end_time
    /// </summary>
    public DateTimeOffset? EndTime { get; init; }

    /// <summary>
    /// calendar_name
    /// </summary>
    public string? CalendarName { get; init; }

    /// <summary>
    /// misfire_instr
    /// </summary>
    public int MisfireInstruction { get; init; }

    /// <summary>
    /// priority
    /// </summary>
    public int Priority { get; init; }


    /// <summary>
    /// job_data
    /// </summary>
    public JobDataMap JobDataMap { get; init; }

    /// <summary>
    /// job_name, job_group
    /// </summary>
    public JobKey JobKey { get; init; }


    protected Trigger(
        ObjectId id,
        string instanceName,
        string name,
        string group,
        string? description,
        DateTimeOffset? nextFireTime,
        DateTimeOffset? previousFireTime,
        LocalTriggerState state,
        DateTimeOffset startTime,
        DateTimeOffset? endTime,
        string? calendarName,
        int misfireInstruction,
        int priority,
        JobDataMap jobDataMap,
        JobKey jobKey
    )
    {
        Id = id;
        InstanceName = instanceName;
        Name = name;
        Group = group;
        Description = description;
        NextFireTime = nextFireTime;
        PreviousFireTime = previousFireTime;
        State = state;
        StartTime = startTime;
        EndTime = endTime;
        CalendarName = calendarName;
        MisfireInstruction = misfireInstruction;
        Priority = priority;
        JobDataMap = jobDataMap;
        JobKey = jobKey;
    }

    protected Trigger(ITrigger trigger, LocalTriggerState state, string instanceName)
    {
        Id = ObjectId.GenerateNewId();
        InstanceName = instanceName;
        Group = trigger.Key.Group;
        Name = trigger.Key.Name;


        JobKey = trigger.JobKey;
        Description = trigger.Description;
        NextFireTime = trigger.GetNextFireTimeUtc();

        PreviousFireTime = trigger.GetPreviousFireTimeUtc();
        State = state;
        StartTime = trigger.StartTimeUtc;
        EndTime = trigger.EndTimeUtc;
        CalendarName = trigger.CalendarName;
        MisfireInstruction = trigger.MisfireInstruction;
        Priority = trigger.Priority;
        JobDataMap = trigger.JobDataMap;
    }


    public abstract IOperableTrigger GetTrigger();

    protected void FillTrigger(AbstractTrigger trigger)
    {
        trigger.Key = new TriggerKey(Name, Group);
        trigger.JobKey = JobKey;
        trigger.CalendarName = CalendarName;
        trigger.Description = Description;
        trigger.JobDataMap = JobDataMap;
        trigger.MisfireInstruction = MisfireInstruction;
        trigger.Priority = Priority;

        trigger.StartTimeUtc = StartTime;
        trigger.EndTimeUtc = EndTime; // EndTimeUtc validates with StartTimeUtc

        trigger.SetNextFireTimeUtc(NextFireTime);
        trigger.SetPreviousFireTimeUtc(PreviousFireTime);
    }

    public TriggerKey GetTriggerKey()
    {
        return new TriggerKey(Name, Group);
    }
}
