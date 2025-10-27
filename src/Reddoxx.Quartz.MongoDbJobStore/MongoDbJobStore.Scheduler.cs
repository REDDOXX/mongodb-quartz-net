using Microsoft.Extensions.Logging;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore;

public partial class MongoDbJobStore
{
    public async Task SchedulerStarted(CancellationToken cancellationToken = default)
    {
        _logger.LogTrace("Scheduler {InstanceId}/{InstanceName} started", InstanceId, InstanceName);

        if (Clustered)
        {
            _clusterManager = new ClusterManager(this);
            await _clusterManager.Initialize();
        }
        else
        {
            try
            {
                await RecoverJobs().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Failure occurred during job recovery: {Message}", ex.Message);

                throw new SchedulerConfigException("Failure occurred during job recovery", ex);
            }
        }

        _misfireHandler = new MisfireHandler(this);
        _misfireHandler.Initialize();
        _schedulerRunning = true;
    }

    public Task SchedulerPaused(CancellationToken token = default)
    {
        _logger.LogTrace("Scheduler {InstanceId}/{InstanceName} paused", InstanceId, InstanceName);
        _schedulerRunning = false;

        return Task.CompletedTask;
    }

    public Task SchedulerResumed(CancellationToken token = default)
    {
        _logger.LogTrace("Scheduler {InstanceId}/{InstanceName} resumed", InstanceId, InstanceName);
        _schedulerRunning = true;

        return Task.CompletedTask;
    }
}
