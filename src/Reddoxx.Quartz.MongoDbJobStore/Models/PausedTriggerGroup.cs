using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

internal class PausedTriggerGroup
{
    [BsonId]
    public ObjectId Id { get; set; }

    /// <summary>
    /// 
    /// </summary>
    /// <remarks>This is also called sched_name</remarks>
    public required string InstanceName { get; set; }

    public required string Group { get; set; }
}
