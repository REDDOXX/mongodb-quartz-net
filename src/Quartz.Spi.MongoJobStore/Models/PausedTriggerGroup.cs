using MongoDB.Bson.Serialization.Attributes;

using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Models;

internal class PausedTriggerGroup
{
    [BsonId]
    public PausedTriggerGroupId Id { get; set; }
}
