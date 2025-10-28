using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;

using Quartz.Impl.AdoJobStore;

using Reddoxx.Quartz.MongoDbJobStore.Models;
using Reddoxx.Quartz.MongoDbJobStore.Util;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class LockRepository : BaseRepository<SchedulerLock>
{
    private readonly ILogger<LockRepository> _logger = LogProvider.CreateLogger<LockRepository>();

    public LockRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "locks", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        // PK_QRTZ_LOCKS
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<SchedulerLock>(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.LockType),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }


    public async Task AcquireLock(
        IClientSessionHandle session,
        QuartzLockType lockType,
        CancellationToken cancellationToken = default
    )
    {
        Exception? initCause = null;

        // attempt lock two times (to work-around possible race conditions in inserting the lock row the first time running)
        var count = 0;

        // Configurable lock retry attempts
        const int maxRetryLocal = 5;
        var retryPeriodLocal = TimeSpan.FromSeconds(1);

        _logger.LogDebug("Lock {InstanceName}/{LockType} tries to acquire lock", InstanceName, lockType);

        do
        {
            count++;

            try
            {
                session.StartTransaction();

                await TryAcquireLock(session, lockType, cancellationToken);

                // obtained lock, go
                return;
            }
            catch (Exception ex)
            {
                if (initCause == null)
                {
                    initCause = ex;
                }

                _logger.LogDebug(
                    "Lock {InstanceName}/{LockType} was not obtained, will_retry={Retry}",
                    InstanceName,
                    lockType,
                    count < maxRetryLocal
                );

                await session.AbortTransactionAsync(cancellationToken);

                if (count < maxRetryLocal)
                {
                    // pause a bit to give another thread some time to commit the insert of the new lock row
                    await Task.Delay(retryPeriodLocal, cancellationToken);

                    // try again ...
                    continue;
                }

                throw new LockException($"Failure obtaining db row lock: {ex.Message}", ex);
            }
        } while (true);
    }

    public async Task TryAcquireLock(
        IClientSessionHandle session,
        QuartzLockType lockType,
        CancellationToken cancellationToken
    )
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & FilterBuilder.Eq(x => x.LockType, lockType);

        var update = UpdateBuilder.Set(x => x.LockKey, ObjectId.GenerateNewId());

        var result = await Collection.FindOneAndUpdateAsync(
            session,
            filter,
            update,
            cancellationToken: cancellationToken
        );

        if (result == null)
        {
            _logger.LogDebug(
                "Inserting new lock row for lock: {InstanceName}/{LockType} being obtained by task",
                InstanceName,
                lockType
            );

            await Collection.InsertOneAsync(
                session,
                new SchedulerLock(InstanceName, lockType, ObjectId.GenerateNewId()),
                cancellationToken: cancellationToken
            );
        }

        _logger.LogDebug("Acquired lock for {InstanceName}/{LockType}", InstanceName, lockType);
    }
}
