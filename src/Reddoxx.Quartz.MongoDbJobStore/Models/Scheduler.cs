using JetBrains.Annotations;

using MongoDB.Bson;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Scheduler
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// sched_name
    /// </summary>
    public string SchedulerName { get; init; }

    /// <summary>
    /// instance_name
    /// </summary>
    public string InstanceId { get; init; }

    /// <summary>
    /// last_checkin_time
    /// </summary>
    public DateTimeOffset LastCheckIn { get; init; }

    /// <summary>
    /// checkin_interval
    /// </summary>
    public TimeSpan CheckInInterval { get; init; }


    public Scheduler(
        ObjectId id,
        string schedulerName,
        string instanceId,
        DateTimeOffset lastCheckIn,
        TimeSpan checkInInterval
    )
    {
        Id = id;
        SchedulerName = schedulerName;
        InstanceId = instanceId;
        LastCheckIn = lastCheckIn;
        CheckInInterval = checkInInterval;
    }


    public Scheduler(string schedulerName, string instanceId)
        : this(schedulerName, instanceId, DateTimeOffset.MinValue, TimeSpan.Zero)
    {
    }

    public Scheduler(string schedulerName, string instanceId, DateTimeOffset lastCheckIn, TimeSpan checkInInterval)
    {
        Id = ObjectId.GenerateNewId();
        SchedulerName = schedulerName;
        InstanceId = instanceId;
        LastCheckIn = lastCheckIn;
        CheckInInterval = checkInInterval;
    }
}
