using MongoDB.Bson;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

internal class PausedTriggerGroup
{
    public ObjectId Id { get; }

    /// <summary>
    /// 
    /// </summary>
    /// <remarks>This is also called sched_name</remarks>
    public string InstanceName { get; }

    public string Group { get; }

    public PausedTriggerGroup(ObjectId id, string instanceName, string group)
    {
        Id = id;
        InstanceName = instanceName;
        Group = group;
    }


    public PausedTriggerGroup(string instanceName, string group)
    {
        Id = ObjectId.GenerateNewId();

        InstanceName = instanceName;
        Group = group;
    }
}
