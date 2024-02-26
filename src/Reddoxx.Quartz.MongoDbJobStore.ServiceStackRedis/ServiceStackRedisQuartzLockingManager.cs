using JetBrains.Annotations;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Locking;
using Reddoxx.Quartz.MongoDbJobStore.Models;

using ServiceStack.Redis;

namespace Reddoxx.Quartz.MongoDbJobStore.ServiceStackRedis;

/// <summary>
/// Quartz locking manager based upon service-stacks redis connector
/// </summary>
[PublicAPI]
public class ServiceStackRedisQuartzLockingManager : IQuartzJobStoreLockingManager
{
    private class RedisLockContext : IQuartzJobStoreLockingManager.ILockContext
    {
        private readonly IRedisClientAsync _client;

        private readonly Stack<IAsyncDisposable> _locks = new();

        public RedisLockContext(IRedisClientAsync client)
        {
            _client = client;
        }

        public async ValueTask DisposeAsync()
        {
            await UnlockResources().ConfigureAwait(false);

            await _client.DisposeAsync().ConfigureAwait(false);
        }

        public async Task TryAcquireLock(
            string schedulerName,
            QuartzLockType lockType,
            CancellationToken cancellationToken
        )
        {
            var key = CreateKey(schedulerName, lockType);

            try
            {
                var acquiredLock = await _client.AcquireLockAsync(key, TimeSpan.FromSeconds(10), cancellationToken)
                    .ConfigureAwait(false);

                _locks.Push(acquiredLock);
            }
            catch (TimeoutException ex)
            {
                throw new JobPersistenceException("Failed to acquire lock in the given timespan", ex);
            }
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
            while (_locks.Count > 0)
            {
                var item = _locks.Pop();

                try
                {
                    await item.DisposeAsync().ConfigureAwait(false);
                }
                catch
                {
                    // Ignored
                }
            }
        }
    }

    private readonly IRedisClientsManagerAsync _manager;

    public ServiceStackRedisQuartzLockingManager(IRedisClientsManagerAsync manager)
    {
        _manager = manager;
    }


    public async Task<IQuartzJobStoreLockingManager.ILockContext> CreateLockContext(CancellationToken cancellationToken)
    {
        var client = await _manager.GetClientAsync(cancellationToken).ConfigureAwait(false);

        return new RedisLockContext(client);
    }

    public async Task<T> ExecuteTransaction<T>(
        string instanceName,
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken
    )
    {
        await using var client = await _manager.GetClientAsync(cancellationToken).ConfigureAwait(false);

        var key = CreateKey(instanceName, lockType);

        await using var _ = await client.AcquireLockAsync(key, token: cancellationToken).ConfigureAwait(false);

        return await txCallback.Invoke().ConfigureAwait(false);
    }


    private static string CreateKey(string schedulerName, QuartzLockType lockType)
    {
        return $"{schedulerName}:{lockType}";
    }
}
