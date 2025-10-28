using JetBrains.Annotations;

using MongoDB.Bson;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[PublicAPI]
public enum QuartzLockType
{
    /// <summary>
    /// Use TRIGGER_ACCESS database locking
    /// </summary>
    TriggerAccess,

    StateAccess,
}

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class SchedulerLock
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// SCHED_NAME
    /// </summary>
    public string InstanceName { get; init; }

    /// <summary>
    /// LOCK_NAME
    /// </summary>
    public QuartzLockType LockType { get; init; }

    /// <summary>
    /// Random lock key which is set when acquiring the lock with findOneAndUpdate.
    /// </summary>
    public ObjectId LockKey { get; init; }


    public SchedulerLock(ObjectId id, string instanceName, QuartzLockType lockType, ObjectId lockKey)
    {
        Id = id;
        InstanceName = instanceName;
        LockType = lockType;
        LockKey = lockKey;
    }

    public SchedulerLock(string instanceName, QuartzLockType lockType, ObjectId lockKey)
    {
        Id = ObjectId.GenerateNewId();
        InstanceName = instanceName;
        LockType = lockType;
        LockKey = lockKey;
    }
}
