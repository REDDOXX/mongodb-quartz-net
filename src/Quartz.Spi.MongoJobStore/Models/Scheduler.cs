using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Scheduler
{
    /// <summary>
    /// sched_name
    /// </summary>
    [BsonRequired]
    public required string SchedulerName { get; set; }

    /// <summary>
    /// instance_name
    /// </summary>
    [BsonRequired]
    public required string InstanceId { get; set; }

    /// <summary>
    /// last_checkin_time
    /// </summary>
    public DateTimeOffset LastCheckIn { get; set; }

    /// <summary>
    /// checkin_interval
    /// </summary>
    public TimeSpan CheckInInterval { get; set; }


    public Scheduler()
    {
    }

    [SetsRequiredMembers]
    public Scheduler(string schedulerName, string instanceId)
    {
        SchedulerName = schedulerName;
        InstanceId = instanceId;
    }
}
