using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Reddoxx.Quartz.MongoDbJobStore.Locking;

internal class MongoDbLockingManager
{
    private class Lock : IAsyncDisposable
    {
        public async ValueTask DisposeAsync()
        {
            // TODO release managed resources here
        }

        public async Task PerformLock()
        {
            await _pendingLocksSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            using var session = await _database.Client.StartSessionAsync(cancellationToken: cancellationToken)
                .ConfigureAwait(false);

            await _lockRepository.AcquireLock(session, lockType, cancellationToken).ConfigureAwait(false);
        }

        public async Task Commit()
        {
            await session.CommitTransactionAsync(cancellationToken).ConfigureAwait(false);
        }
    }
}
