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
    private readonly QueuedTaskScheduler _taskScheduler;
    private Task? _task;


    public MisfireHandler(MongoDbJobStore jobStore)
    {
        _jobStore = jobStore;

        var threadName = $"QuartzScheduler_{_jobStore.InstanceName}-{_jobStore.InstanceId}_MisfireHandler";
        _taskScheduler = new QueuedTaskScheduler(
            threadCount: 1,
            threadName: threadName,
            useForegroundThreads: !_jobStore.MakeThreadsDaemons
        );
    }

    public void Initialize()
    {
        _task = Task.Factory.StartNew(
                () => Run(_cancellationTokenSource.Token),
                CancellationToken.None,
                TaskCreationOptions.HideScheduler,
                _taskScheduler
            )
            .Unwrap();
    }

    public async Task Shutdown()
    {
        Debug.Assert(_task != null);

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

    private async Task Run(CancellationToken cancellationToken)
    {
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var now = DateTime.UtcNow;

            var recoverResult = await Manage().ConfigureAwait(false);
            if (recoverResult.ProcessedMisfiredTriggerCount > 0)
            {
                _jobStore.SignalSchedulingChangeImmediately(recoverResult.EarliestNewTime);
            }

            cancellationToken.ThrowIfCancellationRequested();

            var timeToSleep = TimeSpan.FromMilliseconds(50); // At least a short pause to help balance threads
            if (!recoverResult.HasMoreMisfiredTriggers)
            {
                timeToSleep = _jobStore.MisfireThreshold - (DateTime.UtcNow - now);
                if (timeToSleep <= TimeSpan.Zero)
                {
                    timeToSleep = TimeSpan.FromMilliseconds(50);
                }

                if (_numFails > 0)
                {
                    timeToSleep = _jobStore.DbRetryInterval > timeToSleep ? _jobStore.DbRetryInterval : timeToSleep;
                }
            }

            await Task.Delay(timeToSleep, cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<RecoverMisfiredJobsResult> Manage()
    {
        try
        {
            _logger.LogDebug("Scanning for misfires...");

            var result = await _jobStore.DoRecoverMisfires().ConfigureAwait(false);
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
}
