using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;
using Quartz.Spi.MongoJobStore.Util;

namespace Quartz.Spi.MongoJobStore.Repositories;

[CollectionName("locks")]
internal class LockRepository : BaseRepository<Lock>
{
    private readonly ILogger<LockRepository> _logger = LogProvider.CreateLogger<LockRepository>();

    public LockRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
                new CreateIndexModel<Lock>(
                    IndexBuilder.Ascending(@lock => @lock.AcquiredAt),
                    new CreateIndexOptions
                    {
                        ExpireAfter = TimeSpan.FromSeconds(30),
                    }
                )
            )
            .ConfigureAwait(false);
    }


    public async Task<bool> TryAcquireLock(LockType lockType, string instanceId)
    {
        var lockId = new LockId(lockType, InstanceName);

        _logger.LogTrace("Trying to acquire lock {LockId} on {InstanceId}", lockId, instanceId);
        try
        {
            var @lock = new Lock
            {
                Id = lockId,
                InstanceId = instanceId,
                AcquiredAt = DateTime.Now,
            };

            await Collection.InsertOneAsync(@lock).ConfigureAwait(false);

            _logger.LogTrace("Acquired lock {LockId} on {InstanceId}", lockId, instanceId);
            return true;
        }
        catch (MongoWriteException)
        {
            _logger.LogTrace("Failed to acquire lock {LockId} on {InstanceId}", lockId, instanceId);
            return false;
        }
    }

    public async Task<bool> ReleaseLock(LockType lockType, string instanceId)
    {
        var lockId = new LockId(lockType, InstanceName);
        _logger.LogTrace("Releasing lock {LockId} on {InstanceId}", lockId, instanceId);

        var filter = FilterBuilder.Where(@lock => @lock.Id == lockId && @lock.InstanceId == instanceId);

        var result = await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
        if (result.DeletedCount <= 0)
        {
            _logger.LogWarning(
                "Failed to release lock {LockId} on {InstanceId}. You do not own the lock.",
                lockId,
                instanceId
            );
            return false;
        }

        _logger.LogTrace("Released lock {LockId} on {InstanceId}", lockId, instanceId);
        return true;
    }
}
