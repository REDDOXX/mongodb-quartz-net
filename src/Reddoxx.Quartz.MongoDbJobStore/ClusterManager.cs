using Microsoft.Extensions.Logging;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Util;

namespace Reddoxx.Quartz.MongoDbJobStore;

/// <summary>
/// based upon: https://github.com/quartznet/quartznet/blob/7d525f21a3166c3b595508d345e23313fb9e8d4d/src/Quartz/Impl/AdoJobStore/ClusterManager.cs#L8
/// </summary>
internal class ClusterManager
{
    private readonly ILogger<ClusterManager> _logger = LogProvider.CreateLogger<ClusterManager>();

    // keep constant lock requestor id for manager's lifetime
    private readonly Guid _requestorId = Guid.NewGuid();

    private readonly MongoDbJobStore _jobStore;


    private readonly CancellationTokenSource _cancellationTokenSource = new();

    private QueuedTaskScheduler _taskScheduler = null!;

    private Task _task = null!;

    private int _numFails;

    internal ClusterManager(MongoDbJobStore jobStore)
    {
        _jobStore = jobStore;
    }

    public async Task Initialize()
    {
        await Manage().ConfigureAwait(false);

        var threadName = $"QuartzScheduler_{_jobStore.InstanceName}-{_jobStore.InstanceId}_ClusterManager";

        _taskScheduler = new QueuedTaskScheduler(
            threadCount: 1,
            threadPriority: ThreadPriority.AboveNormal,
            threadName: threadName,
            useForegroundThreads: !_jobStore.MakeThreadsDaemons
        );

        _task = Task.Factory.StartNew(
                () => Run(_cancellationTokenSource.Token),
                _cancellationTokenSource.Token,
                TaskCreationOptions.HideScheduler,
                _taskScheduler
            )
            .Unwrap();
    }

    public async Task Shutdown()
    {
        await _cancellationTokenSource.CancelAsync();
        try
        {
            _taskScheduler.Dispose();

            await _task.ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task Run(CancellationToken token)
    {
        while (true)
        {
            token.ThrowIfCancellationRequested();

            var timeToSleep = _jobStore.ClusterCheckinInterval;
            var transpiredTime = SystemTime.UtcNow() - _jobStore.LastCheckin;
            timeToSleep -= transpiredTime;

            if (timeToSleep <= TimeSpan.Zero)
            {
                timeToSleep = TimeSpan.FromMilliseconds(100);
            }

            if (_numFails > 0)
            {
                timeToSleep = _jobStore.DbRetryInterval > timeToSleep ? _jobStore.DbRetryInterval : timeToSleep;
            }

            await Task.Delay(timeToSleep, token).ConfigureAwait(false);

            token.ThrowIfCancellationRequested();

            if (await Manage().ConfigureAwait(false))
            {
                _jobStore.SignalSchedulingChangeImmediately(MongoDbJobStore.SchedulingSignalDateTime);
            }
        }
    }

    private async ValueTask<bool> Manage()
    {
        var res = false;
        try
        {
            res = await _jobStore.DoCheckin(_requestorId).ConfigureAwait(false);

            _numFails = 0;
            _logger.LogDebug("Check-in complete.");
        }
        catch (Exception e)
        {
            if (_numFails % _jobStore.RetryableActionErrorLogThreshold == 0)
            {
                _logger.LogError(e, "Error managing cluster: {ExceptionMessage}", e.Message);
            }

            _numFails++;
        }

        return res;
    }
}
