using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoJobStore.Models;

internal enum TriggerState
{
    None = 0,
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
[BsonDiscriminator(RootClass = true)]
[BsonKnownTypes(
    typeof(CronTrigger),
    typeof(SimpleTrigger),
    typeof(CalendarIntervalTrigger),
    typeof(DailyTimeIntervalTrigger)
)]
internal abstract class Trigger
{
    [BsonId]
    public ObjectId Id { get; set; }

    /// <summary>
    /// sched_name
    /// </summary>
    public required string InstanceName { get; set; }

    /// <summary>
    /// trigger_name
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// trigger_group
    /// </summary>
    public required string Group { get; set; }


    /// <summary>
    /// description
    /// </summary>
    [BsonIgnoreIfNull]
    public string? Description { get; set; }

    /// <summary>
    /// next_fire_time
    /// </summary>
    [BsonIgnoreIfNull]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? NextFireTime { get; set; }

    /// <summary>
    /// prev_fire_time
    /// </summary>
    [BsonIgnoreIfNull]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? PreviousFireTime { get; set; }

    /// <summary>
    /// trigger_state
    /// </summary>
    [BsonRepresentation(BsonType.String)]
    public TriggerState State { get; set; }

    /// <summary>
    /// start_time
    /// </summary>
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime StartTime { get; set; }

    /// <summary>
    /// end_time
    /// </summary>
    [BsonIgnoreIfNull]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? EndTime { get; set; }

    /// <summary>
    /// calendar_name
    /// </summary>
    [BsonIgnoreIfNull]
    public string? CalendarName { get; set; }

    /// <summary>
    /// misfire_instr
    /// </summary>
    public int MisfireInstruction { get; set; }

    /// <summary>
    /// priority
    /// </summary>
    public int Priority { get; set; }


    /// <summary>
    /// job_data
    /// </summary>
    public JobDataMap JobDataMap { get; set; }

    /// <summary>
    /// job_name, job_group
    /// </summary>
    public JobKey JobKey { get; set; }


    protected Trigger()
    {
    }

    [SetsRequiredMembers]
    protected Trigger(ITrigger trigger, TriggerState state, string instanceName)
    {
        InstanceName = instanceName;
        Group = trigger.Key.Group;
        Name = trigger.Key.Name;


        JobKey = trigger.JobKey;
        Description = trigger.Description;
        NextFireTime = trigger.GetNextFireTimeUtc()?.UtcDateTime;
        PreviousFireTime = trigger.GetPreviousFireTimeUtc()?.UtcDateTime;
        State = state;
        StartTime = trigger.StartTimeUtc.UtcDateTime;
        EndTime = trigger.EndTimeUtc?.UtcDateTime;
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
