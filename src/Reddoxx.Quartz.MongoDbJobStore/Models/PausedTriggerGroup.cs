using JetBrains.Annotations;

using MongoDB.Bson;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class PausedTriggerGroup
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// 
    /// </summary>
    /// <remarks>This is also called sched_name</remarks>
    public string InstanceName { get; init; }

    public string Group { get; init; }

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
