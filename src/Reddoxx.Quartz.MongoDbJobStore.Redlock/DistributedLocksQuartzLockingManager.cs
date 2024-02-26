using JetBrains.Annotations;

using Medallion.Threading.Redis;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Locking;
using Reddoxx.Quartz.MongoDbJobStore.Models;

using StackExchange.Redis;

namespace Reddoxx.Quartz.MongoDbJobStore.Redlock;

[PublicAPI]
public class DistributedLocksQuartzLockingManager : IQuartzJobStoreLockingManager
{
    private class RedisLockContext : IQuartzJobStoreLockingManager.ILockContext
    {
        private readonly IDatabase _database;

        private readonly Stack<RedisDistributedLockHandle> _handles = new();


        public RedisLockContext(IDatabase database)
        {
            _database = database;
        }

        public async ValueTask DisposeAsync()
        {
            await UnlockResources().ConfigureAwait(false);
        }


        public async Task TryAcquireLock(
            string instanceName,
            QuartzLockType lockType,
            CancellationToken cancellationToken
        )
        {
            var key = CreateKey(instanceName, lockType);

            var @lock = new RedisDistributedLock(key, _database);

            var handle = await @lock.TryAcquireAsync(TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
            if (handle == null)
            {
                throw new JobPersistenceException(
                    $"Failed to acquire lock for {instanceName}/{lockType} in the given timespan"
                );
            }

            _handles.Push(handle);
        }

        public Task CommitTransaction(CancellationToken cancellationToken)
        {
            return UnlockResources();
        }

        public Task RollbackTransaction(CancellationToken cancellationToken)
        {
            return UnlockResources();
        }

        private async Task UnlockResources()
        {
            while (_handles.Count > 0)
            {
                var handle = _handles.Pop();

                try
                {
                    await handle.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Ignored
                }
            }
        }
    }

    private readonly IConnectionMultiplexer _multiplexer;

    public DistributedLocksQuartzLockingManager(IConnectionMultiplexer multiplexer)
    {
        _multiplexer = multiplexer;
    }


    public Task<IQuartzJobStoreLockingManager.ILockContext> CreateLockContext(CancellationToken cancellationToken)
    {
        var context = new RedisLockContext(_multiplexer.GetDatabase());

        return Task.FromResult<IQuartzJobStoreLockingManager.ILockContext>(context);
    }

    public async Task<T> ExecuteTransaction<T>(
        string instanceName,
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken
    )
    {
        var key = CreateKey(instanceName, lockType);

        var @lock = new RedisDistributedLock(key, _multiplexer.GetDatabase());

        await using var _ = await @lock.AcquireAsync(null, cancellationToken).ConfigureAwait(false);

        return await txCallback.Invoke().ConfigureAwait(false);
    }


    private static string CreateKey(string instanceName, QuartzLockType lockType)
    {
        return $"{instanceName}:{lockType}";
    }
}
