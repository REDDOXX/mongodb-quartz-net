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


    private readonly MongoDbJobStore _jobStore;

    private readonly CancellationTokenSource _cancellationTokenSource = new();

    private Task? _task;

    private int _numFails;


    internal ClusterManager(MongoDbJobStore jobStore)
    {
        _jobStore = jobStore;
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        await Manage();

        _task = await Task.Factory.StartNew(
            async () => await Run(_cancellationTokenSource.Token),
            cancellationToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        );
    }

    public async Task Shutdown()
    {
        await _cancellationTokenSource.CancelAsync();

        try
        {
            if (_task != null)
            {
                await _task;
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private async Task Run(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (cancellationToken.IsCancellationRequested)
            {
                break;
            }


            var timeToSleep = GetTimeToSleep();

            await Task.Delay(timeToSleep, cancellationToken);


            if (await Manage())
            {
                _jobStore.SignalSchedulingChangeImmediately(MongoDbJobStore.SchedulingSignalDateTime);
            }
        }
    }

    private async ValueTask<bool> Manage()
    {
        try
        {
            var res = await _jobStore.DoCheckIn();

            _numFails = 0;
            _logger.LogDebug("Check-in complete.");

            return res;
        }
        catch (Exception e)
        {
            if (_numFails % _jobStore.RetryableActionErrorLogThreshold == 0)
            {
                _logger.LogError(e, "Error managing cluster: {ExceptionMessage}", e.Message);
            }

            _numFails++;
        }

        return false;
    }

    private TimeSpan GetTimeToSleep()
    {
        var timeToSleep = _jobStore.ClusterCheckinInterval;

        var transpiredTime = SystemTime.UtcNow() - _jobStore.LastCheckin;
        timeToSleep -= transpiredTime;

        if (timeToSleep <= TimeSpan.Zero)
        {
            timeToSleep = TimeSpan.FromMilliseconds(100);
        }

        if (_numFails > 0)
        {
            return _jobStore.DbRetryInterval > timeToSleep ? _jobStore.DbRetryInterval : timeToSleep;
        }

        return timeToSleep;
    }
}
