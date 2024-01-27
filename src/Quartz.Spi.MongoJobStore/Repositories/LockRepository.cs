using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Util;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal class LockRepository : BaseRepository<Lock>
{
    private readonly ILogger<LockRepository> _logger = LogProvider.CreateLogger<LockRepository>();

    public LockRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "locks", instanceName, collectionPrefix)
    {
    }


    public override async Task EnsureIndex()
    {
        // Create: (sched_name,lock_name) uniqueness
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<Lock>(
                Builders<Lock>.IndexKeys.Combine(
                    Builders<Lock>.IndexKeys.Ascending(x => x.InstanceName),
                    Builders<Lock>.IndexKeys.Ascending(x => x.LockType)
                ),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );

        // Auto-unlock after 30 seconds
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
        _logger.LogTrace(
            "Trying to acquire lock {LockType}/{InstanceName} on {InstanceId}",
            lockType,
            InstanceName,
            instanceId
        );
        try
        {
            var @lock = new Lock
            {
                InstanceName = InstanceName,
                LockType = lockType,
                AcquiredAt = DateTime.Now,
            };

            await Collection.InsertOneAsync(@lock).ConfigureAwait(false);

            _logger.LogTrace(
                "Acquired lock {LockType}/{InstanceName} on {InstanceId}",
                lockType,
                InstanceName,
                instanceId
            );
            return true;
        }
        catch (MongoWriteException)
        {
            _logger.LogTrace(
                "Failed to acquire lock {LockType}/{InstanceName} on {InstanceId}",
                lockType,
                InstanceName,
                instanceId
            );
            return false;
        }
    }

    public async Task ReleaseLock(LockType lockType, string instanceId)
    {
        _logger.LogTrace(
            "Releasing lock {LockType}/{InstanceName} on {InstanceId}",
            lockType,
            InstanceName,
            instanceId
        );

        var filter = Builders<Lock>.Filter.Eq(x => x.InstanceName, InstanceName) &
                     Builders<Lock>.Filter.Eq(x => x.LockType, lockType);

        var result = await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
        if (result.DeletedCount <= 0)
        {
            _logger.LogWarning(
                "Failed to release lock {LockType}/{InstanceName} on {InstanceId}. You do not own the lock.",
                lockType,
                InstanceName,
                instanceId
            );
            return;
        }

        _logger.LogTrace("Released lock {LockType}/{InstanceName} on {InstanceId}", lockType, InstanceName, instanceId);
    }
}
