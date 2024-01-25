using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Scheduler
{
    /// <summary>
    /// sched_name
    /// </summary>
    public required string InstanceName { get; set; }

    /// <summary>
    /// instance_name
    /// </summary>
    public required string Name { get; set; }

    /// <summary>
    /// last_checkin_time
    /// </summary>
    public DateTimeOffset LastCheckIn { get; set; }

    /// <summary>
    /// checkin_interval
    /// </summary>
    public TimeSpan CheckInInterval { get; set; }
}
