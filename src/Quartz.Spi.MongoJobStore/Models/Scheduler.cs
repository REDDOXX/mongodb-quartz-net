using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Models;

internal enum SchedulerState
{
    Started,
    Running,
    Paused,
    Resumed,
}

internal class Scheduler
{
    [BsonId]
    public SchedulerId Id { get; set; }

    [BsonRepresentation(BsonType.String)]
    public SchedulerState State { get; set; }

    public DateTime? LastCheckIn { get; set; }
}
