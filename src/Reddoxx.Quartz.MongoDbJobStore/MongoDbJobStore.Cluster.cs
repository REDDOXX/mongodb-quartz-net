using System.Globalization;

using Microsoft.Extensions.Logging;

using Quartz;
using Quartz.Impl.AdoJobStore;
using Quartz.Impl.Triggers;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore;

public partial class MongoDbJobStore
{
    internal async Task<bool> DoCheckIn(CancellationToken cancellationToken = default)
    {
        var recovered = false;


        try
        {
            await _pendingLocksSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            await using var context = await _lockingManager.CreateLockContext(cancellationToken).ConfigureAwait(false);

            try
            {
                // Other than the first time, always checkin first to make sure there is
                // work to be done before we acquire the lock (since that is expensive,
                // and is almost never necessary).  This must be done in a separate
                // transaction to prevent a deadlock under recovery conditions.
                IReadOnlyList<Scheduler>? failedRecords = null;
                if (!_firstCheckIn)
                {
                    failedRecords = await ClusterCheckIn().ConfigureAwait(false);
                }

                if (_firstCheckIn || failedRecords != null && failedRecords.Count > 0)
                {
                    await context.TryAcquireLock(InstanceName, QuartzLockType.StateAccess, cancellationToken)
                        .ConfigureAwait(false);


                    // Now that we own the lock, make sure we still have work to do.
                    // The first time through, we also need to make sure we update/create our state record
                    if (_firstCheckIn)
                    {
                        failedRecords = await ClusterCheckIn().ConfigureAwait(false);
                    }
                    else
                    {
                        failedRecords = await FindFailedInstances().ConfigureAwait(false);
                    }

                    if (failedRecords.Count > 0)
                    {
                        await context.TryAcquireLock(InstanceName, QuartzLockType.TriggerAccess, cancellationToken)
                            .ConfigureAwait(false);

                        await ClusterRecover(failedRecords).ConfigureAwait(false);
                        recovered = true;
                    }
                }

                await context.CommitTransaction(cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await context.RollbackTransaction(cancellationToken).ConfigureAwait(false);

                _logger.LogWarning(ex, "Failed to do cluster check-in {Message}", ex.Message);
            }
        }
        finally
        {
            _pendingLocksSemaphore.Release();
        }

        _firstCheckIn = false;

        return recovered;
    }


    /// <summary>
    /// Get a list of all scheduler instances in the cluster that may have failed.
    /// This includes this scheduler if it is checking in for the first time.
    /// </summary>
    private async Task<List<Scheduler>> FindFailedInstances()
    {
        var failedInstances = new List<Scheduler>();
        var foundThisScheduler = false;

        var schedulers = await _schedulerRepository.SelectSchedulerStateRecords(null).ConfigureAwait(false);

        foreach (var scheduler in schedulers)
        {
            // find own record...
            if (scheduler.InstanceId == InstanceId)
            {
                foundThisScheduler = true;
                if (_firstCheckIn)
                {
                    failedInstances.Add(scheduler);
                }
            }
            else
            {
                // find failed instances...
                if (CalcFailedIfAfter(scheduler) < DateTimeOffset.UtcNow)
                {
                    failedInstances.Add(scheduler);
                }
            }
        }

        // The first time through, also check for orphaned fired triggers.
        if (_firstCheckIn)
        {
            var orphanedInstances = await FindOrphanedFailedInstances(schedulers).ConfigureAwait(false);
            failedInstances.AddRange(orphanedInstances);
        }

        // If not the first time but we didn't find our own instance, then
        // Someone must have done recovery for us.
        if (!foundThisScheduler && !_firstCheckIn)
        {
            // TODO: revisit when handle self-failed-out impl'ed (see TODO in clusterCheckIn() below)
            _logger.LogWarning(
                "This scheduler instance ({InstanceId}) is still active but was recovered by another instance in the cluster. This may cause inconsistent behavior.",
                InstanceId
            );
        }

        return failedInstances;
    }

    /// <summary>
    /// Create dummy <see cref="SchedulerStateRecord" /> objects for fired triggers
    /// that have no scheduler state record. Checkin timestamp and interval are
    /// left as zero on these dummy <see cref="SchedulerStateRecord" /> objects.
    /// </summary>
    /// <param name="schedulers">List of all current <see cref="SchedulerStateRecord" />s</param>
    private async Task<IReadOnlyList<Scheduler>> FindOrphanedFailedInstances(IReadOnlyCollection<Scheduler> schedulers)
    {
        var orphanedInstances = new List<Scheduler>();

        var ids = await _firedTriggerRepository.SelectFiredTriggerInstanceIds().ConfigureAwait(false);

        var allFiredTriggerInstanceIds = new HashSet<string>(ids);
        if (allFiredTriggerInstanceIds.Count > 0)
        {
            foreach (var scheduler in schedulers)
            {
                allFiredTriggerInstanceIds.Remove(scheduler.InstanceId);
            }

            foreach (var instanceId in allFiredTriggerInstanceIds)
            {
                var orphanedInstance = new Scheduler(InstanceName, instanceId);
                orphanedInstances.Add(orphanedInstance);

                _logger.LogWarning(
                    "Found orphaned fired triggers for instance: {SchedulerName}",
                    orphanedInstance.SchedulerName
                );
            }
        }

        return orphanedInstances;
    }


    private DateTimeOffset CalcFailedIfAfter(Scheduler scheduler)
    {
        var passed = DateTimeOffset.UtcNow - LastCheckin;
        var ts = scheduler.CheckInInterval > passed ? scheduler.CheckInInterval : passed; // Max

        return scheduler.LastCheckIn
            //
            .Add(ts)
            .Add(ClusterCheckinMisfireThreshold);
    }

    private async Task<IReadOnlyList<Scheduler>> ClusterCheckIn()
    {
        var failedInstances = await FindFailedInstances().ConfigureAwait(false);

        try
        {
            // TODO: handle self-failed-out

            // check in...
            LastCheckin = SystemTime.UtcNow();

            if (await _schedulerRepository.UpdateState(InstanceId, LastCheckin).ConfigureAwait(false) == 0)
            {
                await _schedulerRepository.AddScheduler(InstanceId, LastCheckin, ClusterCheckinInterval)
                    .ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            throw new JobPersistenceException("Failure updating scheduler state when checking-in: " + e.Message, e);
        }

        return failedInstances;
    }

    private async Task ClusterRecover(IReadOnlyList<Scheduler> failedInstances)
    {
        if (failedInstances.Count <= 0)
        {
            return;
        }

        var recoverIds = SystemTime.UtcNow().Ticks;

        LogWarnIfNonZero(
            failedInstances.Count,
            "ClusterManager: detected {Count} failed or restarted instances.",
            failedInstances.Count
        );

        try
        {
            foreach (var rec in failedInstances)
            {
                _logger.LogInformation(
                    "ClusterManager: Scanning for instance \"{InstanceId}\"'s failed in-progress jobs.",
                    rec.InstanceId
                );


                var firedTriggerRecs = await _firedTriggerRepository.SelectInstancesFiredTriggerRecords(rec.InstanceId)
                    .ConfigureAwait(false);

                var acquiredCount = 0;
                var recoveredCount = 0;
                var otherCount = 0;

                var triggerKeys = new HashSet<TriggerKey>();

                foreach (var ftRec in firedTriggerRecs)
                {
                    var tKey = ftRec.TriggerKey;
                    var jKey = ftRec.JobKey;

                    triggerKeys.Add(tKey);

                    switch (ftRec.State)
                    {
                        // release blocked triggers..
                        case Models.TriggerState.Blocked:
                        {
                            await _triggerRepository.UpdateTriggersStates(
                                    jKey,
                                    Models.TriggerState.Waiting,
                                    Models.TriggerState.Blocked
                                )
                                .ConfigureAwait(false);
                            break;
                        }
                        case Models.TriggerState.PausedBlocked:
                        {
                            await _triggerRepository.UpdateTriggersStates(
                                    jKey,
                                    Models.TriggerState.Paused,
                                    Models.TriggerState.PausedBlocked
                                )
                                .ConfigureAwait(false);
                            break;
                        }
                    }

                    // release acquired triggers..
                    if (ftRec.State == Models.TriggerState.Acquired)
                    {
                        await _triggerRepository.UpdateTriggerState(
                                tKey,
                                Models.TriggerState.Waiting,
                                Models.TriggerState.Acquired
                            )
                            .ConfigureAwait(false);
                        acquiredCount++;
                    }
                    else if (ftRec.RequestsRecovery)
                    {
                        // handle jobs marked for recovery that were not fully
                        // executed...
                        if (await _jobDetailRepository.JobExists(jKey).ConfigureAwait(false))
                        {
                            var recoveryTrig = new SimpleTriggerImpl(
                                $"recover_{rec.InstanceId}_{Convert.ToString(recoverIds++, CultureInfo.InvariantCulture)}",
                                SchedulerConstants.DefaultRecoveryGroup,
                                ftRec.Fired
                            )
                            {
                                JobName = jKey.Name,
                                JobGroup = jKey.Group,
                                MisfireInstruction = MisfireInstruction.SimpleTrigger.FireNow,
                                Priority = ftRec.Priority,
                            };

                            var jd = await _triggerRepository.GetTriggerJobDataMap(tKey).ConfigureAwait(false);
                            if (jd != null)
                            {
                                jd.Put(SchedulerConstants.FailedJobOriginalTriggerName, tKey.Name);
                                jd.Put(SchedulerConstants.FailedJobOriginalTriggerGroup, tKey.Group);
                                jd.Put(
                                    SchedulerConstants.FailedJobOriginalTriggerFiretime,
                                    Convert.ToString(ftRec.Fired, CultureInfo.InvariantCulture)
                                );
                                recoveryTrig.JobDataMap = jd;

                                recoveryTrig.ComputeFirstFireTimeUtc(null);
                                await StoreTriggerInternal(
                                        recoveryTrig,
                                        null,
                                        false,
                                        Models.TriggerState.Waiting,
                                        false,
                                        true
                                    )
                                    .ConfigureAwait(false);
                                recoveredCount++;
                            }
                            else
                            {
                                _logger.LogWarning("");
                                otherCount++;
                            }
                        }
                        else
                        {
                            _logger.LogWarning(
                                "ClusterManager: failed job '{Key}' no longer exists, cannot schedule recovery.",
                                jKey
                            );
                            otherCount++;
                        }
                    }
                    else
                    {
                        otherCount++;
                    }

                    // free up stateful job's triggers
                    if (ftRec.ConcurrentExecutionDisallowed)
                    {
                        await _triggerRepository.UpdateTriggersStates(
                                jKey,
                                Models.TriggerState.Waiting,
                                Models.TriggerState.Blocked
                            )
                            .ConfigureAwait(false);
                        await _triggerRepository.UpdateTriggersStates(
                                jKey,
                                Models.TriggerState.Paused,
                                Models.TriggerState.PausedBlocked
                            )
                            .ConfigureAwait(false);
                    }
                }

                await _firedTriggerRepository.DeleteFiredTriggersByInstanceId(rec.InstanceId).ConfigureAwait(false);

                // Check if any of the fired triggers we just deleted were the last fired trigger
                // records of a COMPLETE trigger.
                var completeCount = 0;
                foreach (var triggerKey in triggerKeys)
                {
                    var triggerState = await _triggerRepository.GetTriggerState(triggerKey).ConfigureAwait(false);
                    if (triggerState == Models.TriggerState.Complete)
                    {
                        var firedTriggers = await _firedTriggerRepository.SelectFiredTriggerRecords(
                                triggerKey.Name,
                                triggerKey.Group
                            )
                            .ConfigureAwait(false);

                        if (firedTriggers.Count == 0)
                        {
                            if (await RemoveTriggerInternal(triggerKey).ConfigureAwait(false))
                            {
                                completeCount++;
                            }
                        }
                    }
                }

                LogWarnIfNonZero(
                    acquiredCount,
                    "ClusterManager: ......Freed {Acquired} acquired trigger(s).",
                    acquiredCount
                );
                LogWarnIfNonZero(
                    completeCount,
                    "ClusterManager: ......Deleted {Completed} complete triggers(s).",
                    completeCount
                );
                LogWarnIfNonZero(
                    recoveredCount,
                    "ClusterManager: ......Scheduled {Recovered} recoverable job(s) for recovery.",
                    recoveredCount
                );
                LogWarnIfNonZero(
                    otherCount,
                    "ClusterManager: ......Cleaned-up {Other} other failed job(s).",
                    otherCount
                );

                if (!string.Equals(rec.InstanceId, InstanceId))
                {
                    await _schedulerRepository.DeleteScheduler(rec.InstanceId).ConfigureAwait(false);
                }
            }
        }
        catch (Exception e)
        {
            throw new JobPersistenceException("Failure recovering jobs: " + e.Message, e);
        }
    }
}
