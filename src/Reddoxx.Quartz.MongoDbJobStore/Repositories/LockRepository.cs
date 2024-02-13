using Microsoft.Extensions.Logging;

using MongoDB.Bson;
using MongoDB.Driver;

using Quartz.Impl.AdoJobStore;

using Reddoxx.Quartz.MongoDbJobStore.Models;
using Reddoxx.Quartz.MongoDbJobStore.Util;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

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
    }

    public async Task AcquireLock(
        IClientSessionHandle session,
        LockType lockType,
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

                var found = false;
                {
                    var filter = Builders<Lock>.Filter.Eq(x => x.InstanceName, InstanceName) &
                                 Builders<Lock>.Filter.Eq(x => x.LockType, lockType);

                    var update = Builders<Lock>.Update.Set(x => x.LockKey, ObjectId.GenerateNewId());


                    var result = await Collection.FindOneAndUpdateAsync(
                            session,
                            filter,
                            update,
                            cancellationToken: cancellationToken
                        )
                        .ConfigureAwait(false);
                    if (result != null)
                    {
                        found = true;
                    }
                }

                if (!found)
                {
                    _logger.LogDebug(
                        "Inserting new lock row for lock: {InstanceName}/{LockType} being obtained by task",
                        InstanceName,
                        lockType
                    );

                    await Collection.InsertOneAsync(
                            session,
                            new Lock
                            {
                                InstanceName = InstanceName,
                                LockType = lockType,
                                LockKey = ObjectId.GenerateNewId(),
                            },
                            cancellationToken: cancellationToken
                        )
                        .ConfigureAwait(false);
                }

                _logger.LogDebug("Acquired lock for {InstanceName}/{LockType}", InstanceName, lockType);

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
                    "Lock  {InstanceName}/{LockType} was not obtained, will_retry={Retry}",
                    InstanceName,
                    lockType,
                    count < maxRetryLocal
                );

                await session.AbortTransactionAsync(cancellationToken).ConfigureAwait(false);

                if (count < maxRetryLocal)
                {
                    // pause a bit to give another thread some time to commit the insert of the new lock row
                    await Task.Delay(retryPeriodLocal, cancellationToken).ConfigureAwait(false);

                    // try again ...
                    continue;
                }

                throw new LockException($"Failure obtaining db row lock: {ex.Message}", ex);
            }
        } while (true);
    }
}
