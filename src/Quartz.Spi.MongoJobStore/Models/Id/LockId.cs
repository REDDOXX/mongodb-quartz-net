using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class LockId
{
    public required string InstanceName { get; set; }

    [BsonRepresentation(BsonType.String)]
    public LockType LockType { get; set; }


    public LockId()
    {
    }

    public LockId(LockType lockType, string instanceName)
    {
        LockType = lockType;
        InstanceName = instanceName;
    }

    public override string ToString()
    {
        return $"{LockType}/{InstanceName}";
    }
}
