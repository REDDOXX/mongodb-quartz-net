using Microsoft.Extensions.Logging;

using Quartz.Impl.AdoJobStore;
using Quartz.Spi.MongoJobStore.Util;

namespace Quartz.Spi.MongoJobStore;

internal class MisfireHandler
{
    private static readonly ILogger Log = LogProvider.CreateLogger<MisfireHandler>();

    private readonly Thread _thread;

    private readonly MongoDbJobStore _jobStore;
    private bool _shutdown;
    private int _numFails;


    public MisfireHandler(MongoDbJobStore jobStore)
    {
        _jobStore = jobStore;

        _thread = new Thread(Run)
        {
            Name = $"QuartzScheduler_{jobStore.InstanceName}-{jobStore.InstanceId}_MisfireHandler",
            IsBackground = true,
        };
    }

    public void Shutdown()
    {
        _shutdown = true;
        _thread.Interrupt();
    }

    public void Start()
    {
        _thread.Start();
    }

    public void Join()
    {
        _thread.Join();
    }

    private void Run()
    {
        while (!_shutdown)
        {
            var now = DateTime.UtcNow;
            var recoverResult = Manage();
            if (recoverResult.ProcessedMisfiredTriggerCount > 0)
            {
                _jobStore.SignalSchedulingChangeImmediately(recoverResult.EarliestNewTime);
            }

            if (!_shutdown)
            {
                var timeToSleep = TimeSpan.FromMilliseconds(50);
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

                try
                {
                    Thread.Sleep(timeToSleep);
                }
                catch (ThreadInterruptedException)
                {
                }
            }
        }
    }

    private RecoverMisfiredJobsResult Manage()
    {
        try
        {
            Log.LogDebug("Scanning for misfires...");
            var result = _jobStore.DoRecoverMisfires().Result;
            _numFails = 0;
            return result;
        }
        catch (Exception ex)
        {
            if (_numFails % _jobStore.RetryableActionErrorLogThreshold == 0)
            {
                Log.LogError(ex, "Error handling misfires: {Message}", ex.Message);
            }

            _numFails++;
        }

        return RecoverMisfiredJobsResult.NoOp;
    }
}
