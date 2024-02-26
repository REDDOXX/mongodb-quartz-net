using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Locking;

internal class JobStoreLockingFactory
{
    private readonly IMongoClient _client;
    private readonly IQuartzJobStoreLockingManager _lockingManager;
    private readonly ILogger<JobStoreLockingFactory> _logger;

    private readonly SemaphoreSlim _pendingLocksSemaphore = new(1);


    public JobStoreLockingFactory(
        IMongoClient client,
        IQuartzJobStoreLockingManager lockingManager,
        ILogger<JobStoreLockingFactory> logger
    )
    {
        _client = client;
        _lockingManager = lockingManager;
        _logger = logger;
    }

    public void AcquireLock()
    {
        if (_lockingManager is MongoDbLockingManager manager)
        {
            manager.AcquireLock();
        }
    }

    public async Task<T> ExecuteTx<T>(
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            await _pendingLocksSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _pendingLocksSemaphore.Release();
        }
    }
}
