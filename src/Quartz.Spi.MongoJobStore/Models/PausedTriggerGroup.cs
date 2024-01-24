namespace Quartz.Spi.MongoJobStore.Models;

internal class PausedTriggerGroup
{
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>This is also called sched_name</remarks>
    public required string InstanceName { get; set; }

    public required string Group { get; set; }
}
