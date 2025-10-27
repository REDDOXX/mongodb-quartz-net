using Microsoft.Extensions.Logging;

using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;

using Reddoxx.Quartz.MongoDbJobStore.Models;

using TriggerState = Quartz.TriggerState;

namespace Reddoxx.Quartz.MongoDbJobStore;

public partial class MongoDbJobStore
{
    public Task StoreTrigger(
        IOperableTrigger newTrigger,
        bool replaceExisting,
        CancellationToken cancellationToken = default
    )
    {
        return ExecuteInTx(
            QuartzLockType.TriggerAccess,
            () => StoreTriggerInternal(newTrigger, null, replaceExisting, Models.TriggerState.Waiting, false, false),
            cancellationToken
        );
    }

    public Task<bool> RemoveTrigger(TriggerKey triggerKey, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => RemoveTriggerInternal(triggerKey), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<bool> RemoveTriggers(IReadOnlyCollection<TriggerKey> triggerKeys, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var result = true;

                    foreach (var triggerKey in triggerKeys)
                    {
                        result = result && await RemoveTriggerInternal(triggerKey);
                    }

                    return result;
                },
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<bool> ReplaceTrigger(
        TriggerKey triggerKey,
        IOperableTrigger newTrigger,
        CancellationToken token = default
    )
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                () => ReplaceTriggerInternal(triggerKey, newTrigger),
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public async Task<IOperableTrigger?> RetrieveTrigger(TriggerKey triggerKey, CancellationToken token = default)
    {
        var result = await _triggerRepository.GetTrigger(triggerKey);

        return result?.GetTrigger();
    }

    public async Task<IReadOnlyCollection<TriggerKey>> GetTriggerKeys(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token = default
    )
    {
        var triggerKeys = await _triggerRepository.GetTriggerKeys(matcher);

        return new HashSet<TriggerKey>(triggerKeys);
    }

    public async Task<IReadOnlyCollection<IOperableTrigger>> GetTriggersForJob(
        JobKey jobKey,
        CancellationToken token = default
    )
    {
        var triggers = await _triggerRepository.GetTriggers(jobKey);

        return triggers.Select(trigger => trigger.GetTrigger())
                       .ToList();
    }

    public async Task<TriggerState> GetTriggerState(TriggerKey triggerKey, CancellationToken token = default)
    {
        var trigger = await _triggerRepository.GetTrigger(triggerKey);

        if (trigger == null)
        {
            return TriggerState.None;
        }

        return trigger.State switch
        {
            Models.TriggerState.Deleted => TriggerState.None,
            Models.TriggerState.Complete => TriggerState.Complete,
            Models.TriggerState.Paused => TriggerState.Paused,
            Models.TriggerState.PausedBlocked => TriggerState.Paused,
            Models.TriggerState.Error => TriggerState.Error,
            Models.TriggerState.Blocked => TriggerState.Blocked,
            _ => TriggerState.Normal,
        };
    }

    public Task ResetTriggerFromErrorState(TriggerKey triggerKey, CancellationToken cancellationToken = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var newState = Models.TriggerState.Waiting;

                    if (await _pausedTriggerGroupRepository.IsTriggerGroupPaused(triggerKey.Group))
                    {
                        newState = Models.TriggerState.Paused;
                    }

                    await _triggerRepository.UpdateTriggerState(triggerKey, newState, Models.TriggerState.Error);

                    _logger.LogInformation(
                        "Trigger {TriggerKey} reset from ERROR state to: {NewState}",
                        triggerKey,
                        newState
                    );
                },
                cancellationToken
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(
                $"Couldn't reset from error state of trigger ({triggerKey}): {ex.Message}",
                ex
            );
        }
    }

    public Task PauseTrigger(TriggerKey triggerKey, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => PauseTriggerInternal(triggerKey), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<IReadOnlyCollection<string>> PauseTriggers(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token = default
    )
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => PauseTriggerGroupInternal(matcher), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task ResumeTrigger(TriggerKey triggerKey, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => ResumeTriggerInternal(triggerKey), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<IReadOnlyCollection<string>> ResumeTriggers(
        GroupMatcher<TriggerKey> matcher,
        CancellationToken token = default
    )
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => ResumeTriggersInternal(matcher), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggers(
        DateTimeOffset noLaterThan,
        int maxCount,
        TimeSpan timeWindow,
        CancellationToken token = default
    )
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                () => AcquireNextTriggersInternal(noLaterThan, maxCount, timeWindow),
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task ReleaseAcquiredTrigger(IOperableTrigger trigger, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    await _triggerRepository.UpdateTriggerState(
                        trigger.Key,
                        Models.TriggerState.Waiting,
                        Models.TriggerState.Acquired
                    );
                    await _triggerRepository.UpdateTriggerState(
                        trigger.Key,
                        Models.TriggerState.Waiting,
                        Models.TriggerState.Blocked
                    );

                    await _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId);
                },
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException($"Couldn't release acquired trigger: {ex.Message}", ex);
        }
    }

    public Task<IReadOnlyCollection<TriggerFiredResult>> TriggersFired(
        IReadOnlyCollection<IOperableTrigger> triggers,
        CancellationToken token = default
    )
    {
        try
        {
            return ExecuteInTx<IReadOnlyCollection<TriggerFiredResult>>(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var results = new List<TriggerFiredResult>();

                    foreach (var operableTrigger in triggers)
                    {
                        TriggerFiredResult result;
                        try
                        {
                            var bundle = await TriggerFiredInternal(operableTrigger);
                            result = new TriggerFiredResult(bundle);
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "Caught exception: {Message}", ex.Message);
                            result = new TriggerFiredResult(ex);
                        }

                        results.Add(result);
                    }

                    return results;
                },
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public async Task TriggeredJobComplete(
        IOperableTrigger trigger,
        IJobDetail jobDetail,
        SchedulerInstruction triggerInstCode,
        CancellationToken token = default
    )
    {
        try
        {
            await ExecuteInTx(
                QuartzLockType.TriggerAccess,
                () => TriggeredJobCompleteInternal(trigger, jobDetail, triggerInstCode),
                token
            );

            var sigTime = ClearAndGetSignalSchedulingChangeOnTxCompletion();
            if (sigTime != null)
            {
                SignalSchedulingChangeImmediately(sigTime);
            }
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    private async Task PauseTriggerInternal(TriggerKey triggerKey)
    {
        var trigger = await _triggerRepository.GetTrigger(triggerKey);

        var state = trigger?.State ?? Models.TriggerState.Deleted;
        switch (state)
        {
            case Models.TriggerState.Waiting:
            case Models.TriggerState.Acquired:
            {
                await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Paused);
                break;
            }
            case Models.TriggerState.Blocked:
            {
                await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.PausedBlocked);
                break;
            }
        }
    }

    private async Task<bool> RemoveTriggerInternal(TriggerKey key, IJobDetail? job = null)
    {
        var trigger = await _triggerRepository.GetTrigger(key);
        if (trigger == null)
        {
            return false;
        }

        if (job == null)
        {
            var result = await _jobDetailRepository.GetJob(trigger.JobKey);
            job = result?.GetJobDetail();
        }

        var removedTrigger = await _triggerRepository.DeleteTrigger(key) > 0;

        if (job != null && !job.Durable)
        {
            if (await _triggerRepository.GetCount(job.Key) == 0)
            {
                if (await RemoveJobInternal(job.Key))
                {
                    await _schedulerSignaler.NotifySchedulerListenersJobDeleted(job.Key);
                }
            }
        }

        return removedTrigger;
    }

    private async Task ResumeTriggerInternal(TriggerKey triggerKey)
    {
        var trigger = await _triggerRepository.GetTrigger(triggerKey);

        if (trigger?.NextFireTime == null || trigger.NextFireTime == DateTime.MinValue)
        {
            return;
        }

        var blocked = trigger.State == Models.TriggerState.PausedBlocked;
        var newState = await CheckBlockedState(trigger.JobKey, Models.TriggerState.Waiting);
        var misfired = false;

        if (_schedulerRunning && trigger.NextFireTime < SystemTime.UtcNow())
        {
            misfired = await UpdateMisfiredTrigger(triggerKey, newState, true);
        }

        if (!misfired)
        {
            var oldState = blocked ? Models.TriggerState.PausedBlocked : Models.TriggerState.Paused;

            await _triggerRepository.UpdateTriggerState(triggerKey, newState, oldState);
        }
    }

    private async Task<IReadOnlyCollection<string>> ResumeTriggersInternal(GroupMatcher<TriggerKey> matcher)
    {
        await _pausedTriggerGroupRepository.DeletePausedTriggerGroup(matcher);
        var groups = new HashSet<string>();

        var keys = await _triggerRepository.GetTriggerKeys(matcher);
        foreach (var triggerKey in keys)
        {
            await ResumeTriggerInternal(triggerKey);
            groups.Add(triggerKey.Group);
        }

        return groups.ToList();
    }


    private async Task<TriggerFiredBundle?> TriggerFiredInternal(IOperableTrigger trigger)
    {
        // Make sure trigger wasn't deleted, paused, or completed...
        var state = await _triggerRepository.GetTriggerState(trigger.Key);
        if (state != Models.TriggerState.Acquired)
        {
            return null;
        }

        JobDetail? job;
        try
        {
            job = await _jobDetailRepository.GetJob(trigger.JobKey);
            if (job == null)
            {
                return null;
            }
        }
        catch (JobPersistenceException ex)
        {
            _logger.LogError(ex, "Error retrieving job, setting trigger state to ERROR.");

            await _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Error);
            throw;
        }

        ICalendar? calendar = null;
        if (trigger.CalendarName != null)
        {
            calendar = await _calendarRepository.GetCalendar(trigger.CalendarName);
            if (calendar == null)
            {
                return null;
            }
        }

        var firedTrigger = new FiredTrigger(
            trigger.FireInstanceId,
            TriggerFactory.CreateTrigger(trigger, Models.TriggerState.Executing, InstanceName),
            job,
            InstanceId,
            Models.TriggerState.Executing
        );
        await _firedTriggerRepository.UpdateFiredTrigger(firedTrigger);

        var prevFireTime = trigger.GetPreviousFireTimeUtc();

        // call triggered - to update the trigger's next-fire-time state...
        trigger.Triggered(calendar);

        state = Models.TriggerState.Waiting;
        var force = true;

        if (job.ConcurrentExecutionDisallowed)
        {
            state = Models.TriggerState.Blocked;
            force = false;
            await _triggerRepository.UpdateTriggersStates(
                trigger.JobKey,
                Models.TriggerState.Blocked,
                Models.TriggerState.Waiting
            );
            await _triggerRepository.UpdateTriggersStates(
                trigger.JobKey,
                Models.TriggerState.Blocked,
                Models.TriggerState.Acquired
            );
            await _triggerRepository.UpdateTriggersStates(
                trigger.JobKey,
                Models.TriggerState.PausedBlocked,
                Models.TriggerState.Paused
            );
        }

        if (!trigger.GetNextFireTimeUtc()
                    .HasValue)
        {
            state = Models.TriggerState.Complete;
            force = true;
        }

        var jobDetail = job.GetJobDetail();
        await StoreTriggerInternal(trigger, jobDetail, true, state, force, force);

        jobDetail.JobDataMap.ClearDirtyFlag();

        return new TriggerFiredBundle(
            jobDetail,
            trigger,
            calendar,
            trigger.Key.Group.Equals(SchedulerConstants.DefaultRecoveryGroup),
            SystemTime.UtcNow(),
            trigger.GetPreviousFireTimeUtc(),
            prevFireTime,
            trigger.GetNextFireTimeUtc()
        );
    }


    private async Task StoreTriggerInternal(
        IOperableTrigger newTrigger,
        IJobDetail? job,
        bool replaceExisting,
        Models.TriggerState state,
        bool forceState,
        bool recovering
    )
    {
        var existingTrigger = await _triggerRepository.TriggerExists(newTrigger.Key);

        if (existingTrigger && !replaceExisting)
        {
            throw new ObjectAlreadyExistsException(newTrigger);
        }

        if (!forceState)
        {
            var shouldBePaused = await _pausedTriggerGroupRepository.IsTriggerGroupPaused(newTrigger.Key.Group);

            if (!shouldBePaused)
            {
                shouldBePaused = await _pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused);
                if (shouldBePaused)
                {
                    await _pausedTriggerGroupRepository.AddPausedTriggerGroup(newTrigger.Key.Group);
                }
            }

            if (shouldBePaused && (state == Models.TriggerState.Waiting || state == Models.TriggerState.Acquired))
            {
                state = Models.TriggerState.Paused;
            }
        }

        if (job == null)
        {
            var jobDetail = await _jobDetailRepository.GetJob(newTrigger.JobKey);
            job = jobDetail?.GetJobDetail();
        }

        if (job == null)
        {
            throw new JobPersistenceException(
                $"The job ({newTrigger.JobKey}) referenced by the trigger does not exist."
            );
        }

        if (job.ConcurrentExecutionDisallowed && !recovering)
        {
            state = await CheckBlockedState(job.Key, state);
        }


        var trigger = TriggerFactory.CreateTrigger(newTrigger, state, InstanceName);

        if (existingTrigger)
        {
            await _triggerRepository.UpdateTrigger(trigger);
        }
        else
        {
            await _triggerRepository.AddTrigger(trigger);
        }
    }


    private async Task<bool> UpdateMisfiredTrigger(
        TriggerKey triggerKey,
        Models.TriggerState newStateIfNotComplete,
        bool forceState
    )
    {
        var trigger = await _triggerRepository.GetTrigger(triggerKey);
        if (trigger == null)
        {
            throw new JobPersistenceException($"No trigger found with id {triggerKey}");
        }

        var misfireTime = SystemTime.UtcNow();
        if (MisfireThreshold > TimeSpan.Zero)
        {
            misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
        }

        if (trigger.NextFireTime.GetValueOrDefault() > misfireTime)
        {
            return false;
        }

        await DoUpdateOfMisfiredTrigger(trigger, forceState, newStateIfNotComplete, false);

        return true;
    }


    private async Task DoUpdateOfMisfiredTrigger(
        Trigger trigger,
        bool forceState,
        Models.TriggerState newStateIfNotComplete,
        bool recovering
    )
    {
        var operableTrigger = trigger.GetTrigger();

        ICalendar? cal = null;
        if (trigger.CalendarName != null)
        {
            cal = await _calendarRepository.GetCalendar(trigger.CalendarName);
        }

        await _schedulerSignaler.NotifyTriggerListenersMisfired(operableTrigger);
        operableTrigger.UpdateAfterMisfire(cal);

        if (!operableTrigger.GetNextFireTimeUtc()
                            .HasValue)
        {
            await StoreTriggerInternal(
                operableTrigger,
                null,
                true,
                Models.TriggerState.Complete,
                forceState,
                recovering
            );
            await _schedulerSignaler.NotifySchedulerListenersFinalized(operableTrigger);
        }
        else
        {
            await StoreTriggerInternal(operableTrigger, null, true, newStateIfNotComplete, forceState, recovering);
        }
    }


    private async Task<IReadOnlyCollection<IOperableTrigger>> AcquireNextTriggersInternal(
        DateTimeOffset noLaterThan,
        int maxCount,
        TimeSpan timeWindow
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeWindow, TimeSpan.Zero);

        var acquiredTriggers = new List<IOperableTrigger>();
        var acquiredJobKeysForNoConcurrentExec = new HashSet<JobKey>();

        const int maxDoLoopRetry = 3;
        var currentLoopCount = 0;

        do
        {
            currentLoopCount++;

            var triggerKeys = await _triggerRepository.GetTriggersToAcquire(
                noLaterThan + timeWindow,
                MisfireTime,
                maxCount
            );

            if (triggerKeys.Count == 0)
            {
                return acquiredTriggers;
            }


            var batchEnd = noLaterThan;

            foreach (var triggerKey in triggerKeys)
            {
                // If our trigger is no longer available, try a new one.
                var nextTrigger = await _triggerRepository.GetTrigger(triggerKey);
                if (nextTrigger == null)
                {
                    continue; // next trigger
                }

                // In the Ado version we'd join the TRIGGERS with JOB_DETAILS. 
                var jobKey = nextTrigger.JobKey;

                JobDetail? jobDetail;
                try
                {
                    jobDetail = await _jobDetailRepository.GetJob(jobKey);
                    if (jobDetail == null)
                    {
                        throw new JobPersistenceException($"No job details found for trigger {triggerKey}");
                    }
                }
                catch (Exception)
                {
                    await _triggerRepository.UpdateTriggerState(triggerKey, Models.TriggerState.Error);
                    continue;
                }

                // If trigger's job is set as @DisallowConcurrentExecution, and it has already been added to result, then
                // put it back into the timeTriggers set and continue to search for next trigger.

                if (jobDetail.ConcurrentExecutionDisallowed)
                {
                    if (!acquiredJobKeysForNoConcurrentExec.Add(jobKey))
                    {
                        continue; // next trigger
                    }
                }


                var nextFireTimeUtc = nextTrigger.NextFireTime;

                // A trigger should not return NULL on nextFireTime when fetched from DB.
                // But for whatever reason if we do have this (BAD trigger implementation or
                // data?), we then should log a warning and continue to next trigger.
                // User would need to manually fix these triggers from DB as they will not
                // able to be clean up by Quartz since we are not returning it to be processed.
                if (nextFireTimeUtc == null)
                {
                    _logger.LogWarning(
                        "Trigger {Key} returned null on nextFireTime and yet still exists in DB!",
                        nextTrigger.GetTriggerKey()
                    );
                    continue;
                }

                if (nextFireTimeUtc > batchEnd)
                {
                    break;
                }

                // We now have a acquired trigger, let's add to return list.
                // If our trigger was no longer in the expected state, try a new one.
                var result = await _triggerRepository.UpdateTriggerState(
                    triggerKey,
                    Models.TriggerState.Acquired,
                    Models.TriggerState.Waiting
                );

                if (result <= 0)
                {
                    continue;
                }

                var operableTrigger = nextTrigger.GetTrigger();
                operableTrigger.FireInstanceId = GetFiredTriggerRecordId();


                var firedTrigger = new FiredTrigger(
                    operableTrigger.FireInstanceId,
                    nextTrigger,
                    null,
                    InstanceId,
                    Models.TriggerState.Acquired
                );

                await _firedTriggerRepository.AddFiredTrigger(firedTrigger);

                if (acquiredTriggers.Count == 0)
                {
                    var now = SystemTime.UtcNow();
                    var nextFireTime = nextFireTimeUtc.Value;
                    var max = now > nextFireTime ? now : nextFireTime;

                    batchEnd = max + timeWindow;
                }

                acquiredTriggers.Add(operableTrigger);
            }

            // if we didn't end up with any trigger to fire from that first
            // batch, try again for another batch. We allow with a max retry count.

            if (acquiredTriggers.Count == 0 && currentLoopCount < maxDoLoopRetry)
            {
                continue;
            }

            // We are done with the while loop.
            break;
        } while (true);

        return acquiredTriggers;
    }

    private string GetFiredTriggerRecordId()
    {
        Interlocked.Increment(ref _fireTriggerRecordCounter);
        return InstanceId + _fireTriggerRecordCounter;
    }


    private async Task<Models.TriggerState> CheckBlockedState(JobKey jobKey, Models.TriggerState currentState)
    {
        // State can only transition to BLOCKED from PAUSED or WAITING.
        if (currentState != Models.TriggerState.Waiting && currentState != Models.TriggerState.Paused)
        {
            return currentState;
        }

        var firedTriggers = await _firedTriggerRepository.GetFiredTriggers(jobKey);

        var firedTrigger = firedTriggers.FirstOrDefault();
        if (firedTrigger != null)
        {
            if (firedTrigger.ConcurrentExecutionDisallowed) // TODO: worry about failed/recovering/volatile job  states?
            {
                return currentState == Models.TriggerState.Paused ? Models.TriggerState.PausedBlocked
                    : Models.TriggerState.Blocked;
            }
        }

        return currentState;
    }
}
