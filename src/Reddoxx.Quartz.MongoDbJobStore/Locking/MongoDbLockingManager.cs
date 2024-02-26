using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Models;
using Reddoxx.Quartz.MongoDbJobStore.Repositories;

namespace Reddoxx.Quartz.MongoDbJobStore.Locking;

internal class MongoDbLockingManager : IQuartzJobStoreLockingManager
{
    private class MongoLockContext : IQuartzJobStoreLockingManager.ILockContext
    {
        private readonly IClientSessionHandle _session;

        private readonly LockRepository _lockRepository;

        private readonly ILogger _logger;


        public MongoLockContext(IClientSessionHandle session, LockRepository lockRepository, ILogger logger)
        {
            _session = session;
            _lockRepository = lockRepository;
            _logger = logger;
        }

        public async ValueTask DisposeAsync()
        {
            await RollbackTransaction(CancellationToken.None).ConfigureAwait(false);

            _session.Dispose();
        }

        public async Task TryAcquireLock(
            string schedulerName,
            QuartzLockType lockType,
            CancellationToken cancellationToken
        )
        {
            await _lockRepository.TryAcquireLock(_session, lockType, cancellationToken).ConfigureAwait(false);
        }

        public async Task CommitTransaction(CancellationToken cancellationToken)
        {
            if (_session.IsInTransaction)
            {
                try
                {
                    await _session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    _logger.LogWarning(ex, "Failed to commit transaction {Message}", ex.Message);
                    throw;
                }
            }
        }

        public async Task RollbackTransaction(CancellationToken cancellationToken)
        {
            if (_session.IsInTransaction)
            {
                await _session.AbortTransactionAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }


    private readonly IMongoClient _client;

    private readonly LockRepository _lockRepository;

    private readonly ILogger<MongoDbLockingManager> _logger;


    public MongoDbLockingManager(
        IMongoClient client,
        LockRepository lockRepository,
        ILogger<MongoDbLockingManager> logger
    )
    {
        _client = client;
        _lockRepository = lockRepository;
        _logger = logger;
    }


    public async Task<IQuartzJobStoreLockingManager.ILockContext> CreateLockContext(CancellationToken cancellationToken)
    {
        var session = await _client.StartSessionAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        session.StartTransaction();

        return new MongoLockContext(session, _lockRepository, _logger);
    }

    public async Task<T> ExecuteTransaction<T>(
        string instanceName,
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken
    )
    {
        using var session = await _client.StartSessionAsync(cancellationToken: cancellationToken).ConfigureAwait(false);

        await _lockRepository.AcquireLock(session, lockType, cancellationToken).ConfigureAwait(false);

        try
        {
            var result = await txCallback.Invoke().ConfigureAwait(false);

            try
            {
                await session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Failed to commit transaction {Message}", ex.Message);
            }

            return result;
        }
        catch
        {
            await session.AbortTransactionAsync(cancellationToken).ConfigureAwait(false);
            throw;
        }
    }
}
