using System.Collections.Concurrent;
using System.Diagnostics;

using Microsoft.Extensions.Logging;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Repositories;
using Quartz.Spi.MongoJobStore.Util;

namespace Quartz.Spi.MongoJobStore;

/// <summary>
/// Implements a simple distributed lock on top of MongoDB. It is not a reentrant lock so you can't
/// acquire the lock more than once in the same thread of execution.
/// </summary>
internal class LockManager : IDisposable
{
    private class LockInstance : IDisposable, IAsyncDisposable
    {
        private readonly LockManager _lockManager;


        private bool _disposed;

        public string InstanceId { get; }

        public LockType LockType { get; }


        public LockInstance(LockManager lockManager, LockType lockType, string instanceId)
        {
            _lockManager = lockManager;
            LockType = lockType;
            InstanceId = instanceId;
        }


        public void Dispose()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(
                    nameof(LockInstance),
                    $"This lock {LockType} for {InstanceId} has already been disposed"
                );
            }

            _lockManager.ReleaseLock(this).GetAwaiter().GetResult();

            _disposed = true;
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
            {
                throw new ObjectDisposedException(
                    nameof(LockInstance),
                    $"This lock {LockType} for {InstanceId} has already been disposed"
                );
            }

            await _lockManager.ReleaseLock(this).ConfigureAwait(false);

            _disposed = true;
        }
    }

    private static readonly TimeSpan SleepThreshold = TimeSpan.FromMilliseconds(1000);

    private readonly ILogger _logger = LogProvider.CreateLogger<LockManager>();

    private readonly LockRepository _lockRepository;

    private readonly ConcurrentDictionary<LockType, LockInstance> _pendingLocks = new();

    private readonly SemaphoreSlim _pendingLocksSemaphore = new(1);

    private bool _disposed;

    public LockManager(LockRepository lockRepository)
    {
        _lockRepository = lockRepository;
    }

    public void Dispose()
    {
        ThrowIfNotDisposed();

        _disposed = true;

        var locks = _pendingLocks.ToArray();
        foreach (var keyValuePair in locks)
        {
            keyValuePair.Value.Dispose();
        }
    }

    public async Task<IAsyncDisposable> AcquireLock(LockType lockType, string instanceId)
    {
        while (true)
        {
            ThrowIfNotDisposed();

            await _pendingLocksSemaphore.WaitAsync();
            try
            {
                if (await _lockRepository.TryAcquireLock(lockType, instanceId).ConfigureAwait(false))
                {
                    var lockInstance = new LockInstance(this, lockType, instanceId);
                    AddLock(lockInstance);

                    return lockInstance;
                }
            }
            finally
            {
                _pendingLocksSemaphore.Release();
            }

            await Task.Delay(SleepThreshold);
        }
    }

    private async Task ReleaseLock(LockInstance lockInstance)
    {
        await _pendingLocksSemaphore.WaitAsync();
        try
        {
            await _lockRepository.ReleaseLock(lockInstance.LockType, lockInstance.InstanceId).ConfigureAwait(false);

            LockReleased(lockInstance);
        }
        finally
        {
            _pendingLocksSemaphore.Release();
        }
    }


    private void AddLock(LockInstance lockInstance)
    {
        if (!_pendingLocks.TryAdd(lockInstance.LockType, lockInstance))
        {
            throw new Exception(
                $"Unable to add lock instance for lock {lockInstance.LockType} on {lockInstance.InstanceId}"
            );
        }
    }

    private void LockReleased(LockInstance lockInstance)
    {
        if (!_pendingLocks.TryRemove(lockInstance.LockType, out _))
        {
            _logger.LogWarning(
                "Unable to remove pending lock {LockType} on {InstanceId}",
                lockInstance.LockType,
                lockInstance.InstanceId
            );
        }
    }

    [DebuggerStepThrough]
    private void ThrowIfNotDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(LockManager));
        }
    }
}
