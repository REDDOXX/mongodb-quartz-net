using Microsoft.Extensions.Logging;

using Quartz;
using Quartz.Impl.AdoJobStore;

using Reddoxx.Quartz.MongoDbJobStore.Models;

using TriggerState = Reddoxx.Quartz.MongoDbJobStore.Models.TriggerState;

namespace Reddoxx.Quartz.MongoDbJobStore;

public partial class MongoDbJobStore
{
    internal async Task<RecoverMisfiredJobsResult> DoRecoverMisfires()
    {
        try
        {
            var misfireCount = await _triggerRepository.GetMisfireCount(MisfireTime.UtcDateTime);

            _logger.LogDebug("Found {MisfireCount} triggers that missed their scheduled fire-time.", misfireCount);

            if (misfireCount == 0)
            {
                return RecoverMisfiredJobsResult.NoOp;
            }

            return await ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () => await RecoverMisfiredJobsInternal(false)
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    private Task RecoverJobs()
    {
        return ExecuteInTx(QuartzLockType.TriggerAccess, RecoverJobsInternal);
    }


    private async Task RecoverJobsInternal()
    {
        // update inconsistent job states
        var result = await _triggerRepository.UpdateTriggersStates(
            TriggerState.Waiting,
            TriggerState.Acquired,
            TriggerState.Blocked
        );

        result += await _triggerRepository.UpdateTriggersStates(TriggerState.Paused, TriggerState.PausedBlocked);

        _logger.LogInformation("Freed {Count} triggers from 'acquired' / 'blocked' state.", result);

        // clean up misfired jobs
        await RecoverMisfiredJobsInternal(true);

        var recoverableFiredTriggers = await _firedTriggerRepository.GetRecoverableFiredTriggers(InstanceId);

        var results = recoverableFiredTriggers.Select(async trigger =>
            trigger.GetRecoveryTrigger(await _triggerRepository.GetTriggerJobDataMap(trigger.TriggerKey))
        );

        // recover jobs marked for recovery that were not fully executed
        var recoveringJobTriggers = (await Task.WhenAll(results)).ToList();

        _logger.LogInformation(
            "Recovering {Count} jobs that were in-progress at the time of the last shut-down.",
            recoveringJobTriggers.Count
        );

        foreach (var recoveringJobTrigger in recoveringJobTriggers)
        {
            if (await _jobDetailRepository.JobExists(recoveringJobTrigger.JobKey))
            {
                recoveringJobTrigger.ComputeFirstFireTimeUtc(null);

                await StoreTriggerInternal(recoveringJobTrigger, null, false, TriggerState.Waiting, false, true);
            }
        }

        _logger.LogInformation("Recovery complete");

        var completedTriggers = await _triggerRepository.GetTriggerKeys(TriggerState.Complete);

        foreach (var completedTrigger in completedTriggers)
        {
            await RemoveTriggerInternal(completedTrigger);
        }

        _logger.LogInformation("Removed {Count} 'complete' triggers.", completedTriggers.Count);

        // clean up any fired trigger entries
        result = await _firedTriggerRepository.DeleteFiredTriggers();
        _logger.LogInformation("Removed {Count} stale fired job entries.", result);
    }


    private async Task<RecoverMisfiredJobsResult> RecoverMisfiredJobsInternal(bool recovering)
    {
        // If recovering, we want to handle all of the misfired
        // triggers right away.
        var maxMisfiresToHandleAtTime = recovering ? -1 : MaxMisfiresToHandleAtATime;

        var (hasMoreMisfiredTriggers, misfiredTriggers) = await _triggerRepository.HasMisfiredTriggers(
            MisfireTime,
            maxMisfiresToHandleAtTime
        );

        if (hasMoreMisfiredTriggers)
        {
            _logger.LogInformation(
                "Handling the first {Count} triggers that missed their scheduled fire-time.  " +
                "More misfired triggers remain to be processed.",
                misfiredTriggers.Count
            );
        }
        else if (misfiredTriggers.Count > 0)
        {
            _logger.LogInformation(
                "Handling {Count} trigger(s) that missed their scheduled fire-time.",
                misfiredTriggers.Count
            );
        }
        else
        {
            _logger.LogInformation("Found 0 triggers that missed their scheduled fire-time.");
            return RecoverMisfiredJobsResult.NoOp;
        }

        var earliestNewTime = DateTimeOffset.MaxValue;

        foreach (var misfiredTrigger in misfiredTriggers)
        {
            var trigger = await _triggerRepository.GetTrigger(misfiredTrigger);
            if (trigger == null)
            {
                continue;
            }

            await DoUpdateOfMisfiredTrigger(trigger, false, TriggerState.Waiting, recovering);

            var nextTime = trigger.NextFireTime;
            if (nextTime.HasValue && nextTime.Value < earliestNewTime)
            {
                earliestNewTime = nextTime.Value;
            }
        }

        return new RecoverMisfiredJobsResult(hasMoreMisfiredTriggers, misfiredTriggers.Count, earliestNewTime);
    }
}
