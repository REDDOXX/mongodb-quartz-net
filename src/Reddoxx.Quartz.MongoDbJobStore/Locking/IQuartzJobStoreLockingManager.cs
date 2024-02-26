using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Locking;

public interface IQuartzJobStoreLockingManager
{
    public interface ILockContext : IAsyncDisposable
    {
        Task TryAcquireLock(string instanceName, QuartzLockType lockType, CancellationToken cancellationToken);

        Task CommitTransaction(CancellationToken cancellationToken);

        Task RollbackTransaction(CancellationToken cancellationToken);
    }

    /// <summary>
    /// Creates a lockable context, which can acquire multiple locks. This is used in the cluster-check-in
    /// to first acquire the state access and then the trigger access locks.
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task<ILockContext> CreateLockContext(CancellationToken cancellationToken);


    /// <summary>
    /// Runs <paramref name="txCallback"/> globally locked. The global locking key is (scheduler_name, lock_type).
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <param name="instanceName"></param>
    /// <param name="lockType"></param>
    /// <param name="txCallback"></param>
    /// <param name="cancellationToken"></param>
    /// <returns></returns>
    Task<T> ExecuteTransaction<T>(
        string instanceName,
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken
    );
}
