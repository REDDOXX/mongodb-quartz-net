using Microsoft.Extensions.Logging;

using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Spi;

using Reddoxx.Quartz.MongoDbJobStore.Models;
using Reddoxx.Quartz.MongoDbJobStore.Util;

namespace Reddoxx.Quartz.MongoDbJobStore;

public partial class MongoDbJobStore
{
    public Task StoreJob(IJobDetail newJob, bool replaceExisting, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => StoreJobInternal(newJob, replaceExisting), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    public Task<bool> RemoveJob(JobKey jobKey, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => RemoveJobInternal(jobKey), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<bool> RemoveJobs(IReadOnlyCollection<JobKey> jobKeys, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var result = true;

                    foreach (var jobKey in jobKeys)
                    {
                        result = result && await RemoveJobInternal(jobKey);
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


    public async Task<IJobDetail?> RetrieveJob(JobKey jobKey, CancellationToken token = default)
    {
        var result = await _jobDetailRepository.GetJob(jobKey);

        return result?.GetJobDetail();
    }

    public Task PauseJob(JobKey jobKey, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var triggers = await GetTriggersForJob(jobKey, token);

                    foreach (var operableTrigger in triggers)
                    {
                        await PauseTriggerInternal(operableTrigger.Key);
                    }
                },
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    public Task<IReadOnlyCollection<string>> PauseJobs(GroupMatcher<JobKey> matcher, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx<IReadOnlyCollection<string>>(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var jobKeys = await _jobDetailRepository.GetJobsKeys(matcher);

                    var groupNames = new HashSet<string>();
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = await _triggerRepository.GetTriggers(jobKey);

                        foreach (var trigger in triggers)
                        {
                            await PauseTriggerInternal(trigger.GetTriggerKey());
                        }

                        groupNames.Add(jobKey.Group);
                    }

                    return groupNames;
                },
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    private async Task<bool> RemoveJobInternal(JobKey jobKey)
    {
        await _triggerRepository.DeleteTriggers(jobKey);

        var result = await _jobDetailRepository.DeleteJob(jobKey);
        return result > 0;
    }


    private async Task StoreJobInternal(IJobDetail newJob, bool replaceExisting)
    {
        var existingJob = await _jobDetailRepository.JobExists(newJob.Key);

        var jobDetail = new JobDetail(newJob, InstanceName);

        if (existingJob)
        {
            if (!replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newJob);
            }

            await _jobDetailRepository.UpdateJob(jobDetail);
        }
        else
        {
            await _jobDetailRepository.AddJob(jobDetail);
        }
    }

    private async Task TriggeredJobCompleteInternal(
        IOperableTrigger trigger,
        IJobDetail jobDetail,
        SchedulerInstruction triggerInstCode
    )
    {
        try
        {
            switch (triggerInstCode)
            {
                case SchedulerInstruction.DeleteTrigger:
                {
                    if (!trigger.GetNextFireTimeUtc()
                                .HasValue)
                    {
                        // double check for possible reschedule within job
                        // execution, which would cancel the need to delete...
                        var trig = await _triggerRepository.GetTrigger(trigger.Key);
                        if (trig != null && !trig.NextFireTime.HasValue)
                        {
                            await RemoveTriggerInternal(trigger.Key, jobDetail);
                        }
                    }
                    else
                    {
                        await RemoveTriggerInternal(trigger.Key, jobDetail);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    }

                    break;
                }
                case SchedulerInstruction.SetTriggerComplete:
                {
                    await _triggerRepository.UpdateTriggerState(trigger.Key, LocalTriggerState.Complete);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
                case SchedulerInstruction.SetTriggerError:
                {
                    _logger.LogInformation("Trigger {Key} set to ERROR state.", trigger.Key);
                    await _triggerRepository.UpdateTriggerState(trigger.Key, LocalTriggerState.Error);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
                case SchedulerInstruction.SetAllJobTriggersComplete:
                {
                    await _triggerRepository.UpdateTriggersStates(trigger.JobKey, LocalTriggerState.Complete);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
                case SchedulerInstruction.SetAllJobTriggersError:
                {
                    _logger.LogInformation("All triggers of Job {JobKey} set to ERROR state.", trigger.JobKey);
                    await _triggerRepository.UpdateTriggersStates(trigger.JobKey, LocalTriggerState.Error);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
            }

            if (jobDetail.ConcurrentExecutionDisallowed)
            {
                await _triggerRepository.UpdateTriggersStates(
                    jobDetail.Key,
                    LocalTriggerState.Waiting,
                    LocalTriggerState.Blocked
                );

                await _triggerRepository.UpdateTriggersStates(
                    jobDetail.Key,
                    LocalTriggerState.Paused,
                    LocalTriggerState.PausedBlocked
                );
                SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
            }

            if (jobDetail.PersistJobDataAfterExecution && jobDetail.JobDataMap.Dirty)
            {
                await _jobDetailRepository.UpdateJobData(jobDetail.Key, jobDetail.JobDataMap);
            }
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }

        try
        {
            await _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    protected virtual void SignalSchedulingChangeOnTxCompletion(DateTimeOffset candidateNewNextFireTime)
    {
        var sigTime = LogicalThreadContext.GetData<DateTimeOffset?>(KeySignalChangeForTxCompletion);
        if (sigTime == null)
        {
            LogicalThreadContext.SetData(KeySignalChangeForTxCompletion, candidateNewNextFireTime);
        }
        else
        {
            if (candidateNewNextFireTime < sigTime)
            {
                LogicalThreadContext.SetData(KeySignalChangeForTxCompletion, candidateNewNextFireTime);
            }
        }
    }
}
