using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Scheduler
{
    public required string Name { get; set; }

    public required string InstanceName { get; set; }


    public DateTimeOffset LastCheckIn { get; set; }

    public TimeSpan CheckInInterval { get; set; }
}
