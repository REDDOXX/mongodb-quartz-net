using JetBrains.Annotations;

namespace Reddoxx.Quartz.MongoDbJobStore.Locking;

[PublicAPI]
public interface IQuartzJobStoreLockingManager
{
    Task AcquireLock(string instanceName, string lockType);
}
