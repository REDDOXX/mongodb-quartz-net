using System.Diagnostics;

using Microsoft.Extensions.Logging;

using Quartz.Impl.AdoJobStore;

using Reddoxx.Quartz.MongoDbJobStore.Util;

namespace Reddoxx.Quartz.MongoDbJobStore;

internal class MisfireHandler
{
    private readonly ILogger _logger = LogProvider.CreateLogger<MisfireHandler>();

    private readonly MongoDbJobStore _jobStore;
    private int _numFails;

    private readonly CancellationTokenSource _cancellationTokenSource = new();
    private Task? _task;


    public MisfireHandler(MongoDbJobStore jobStore)
    {
        _jobStore = jobStore;
    }

    public async Task Initialize(CancellationToken cancellationToken = default)
    {
        _task = await Task.Factory.StartNew(
            async () => await Run(_cancellationTokenSource.Token),
            cancellationToken,
            TaskCreationOptions.LongRunning,
            TaskScheduler.Default
        );
    }

    public async Task Shutdown()
    {
        Debug.Assert(_task != null);

        await _cancellationTokenSource.CancelAsync();

        try
        {
            await _task;
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

            var now = DateTime.UtcNow;

            var recoverResult = await Manage();
            if (recoverResult.ProcessedMisfiredTriggerCount > 0)
            {
                _jobStore.SignalSchedulingChangeImmediately(recoverResult.EarliestNewTime);
            }

            var timeToSleep = GetTimeToSleep(recoverResult, now);

            await Task.Delay(timeToSleep, cancellationToken);
        }
    }


    private async Task<RecoverMisfiredJobsResult> Manage()
    {
        try
        {
            _logger.LogDebug("Scanning for misfires...");

            var result = await _jobStore.DoRecoverMisfires();
            _numFails = 0;
            return result;
        }
        catch (Exception ex)
        {
            if (_numFails % _jobStore.RetryableActionErrorLogThreshold == 0)
            {
                _logger.LogError(ex, "Error handling misfires: {Message}", ex.Message);
            }

            _numFails++;
        }

        return RecoverMisfiredJobsResult.NoOp;
    }

    private TimeSpan GetTimeToSleep(RecoverMisfiredJobsResult recoverResult, DateTime now)
    {
        var timeToSleep = TimeSpan.FromMilliseconds(50); // At least a short pause to help balance threads
        if (recoverResult.HasMoreMisfiredTriggers)
        {
            return timeToSleep;
        }

        timeToSleep = _jobStore.MisfireThreshold - (DateTime.UtcNow - now);

        if (timeToSleep <= TimeSpan.Zero)
        {
            timeToSleep = TimeSpan.FromMilliseconds(50);
        }

        if (_numFails > 0)
        {
            return _jobStore.DbRetryInterval > timeToSleep ? _jobStore.DbRetryInterval : timeToSleep;
        }

        return timeToSleep;
    }
}
