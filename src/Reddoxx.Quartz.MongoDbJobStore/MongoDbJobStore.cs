using JetBrains.Annotations;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using Quartz;
using Quartz.Impl.Matchers;
using Quartz.Simpl;
using Quartz.Spi;

using Reddoxx.Quartz.MongoDbJobStore.Database;
using Reddoxx.Quartz.MongoDbJobStore.Locking;
using Reddoxx.Quartz.MongoDbJobStore.Models;
using Reddoxx.Quartz.MongoDbJobStore.Repositories;
using Reddoxx.Quartz.MongoDbJobStore.Serializers;
using Reddoxx.Quartz.MongoDbJobStore.Util;

namespace Reddoxx.Quartz.MongoDbJobStore;

[PublicAPI]
public partial class MongoDbJobStore : IJobStore
{
    internal static readonly SystemTextJsonObjectSerializer ObjectSerializer = new();

    private const string KeySignalChangeForTxCompletion = "sigChangeForTxCompletion";
    private const string AllGroupsPaused = "_$_ALL_GROUPS_PAUSED_$_";

    internal static readonly DateTimeOffset SchedulingSignalDateTime =
        new DateTimeOffset(1982, 6, 28, 0, 0, 0, TimeSpan.FromSeconds(0));

    private static long _fireTriggerRecordCounter = DateTime.UtcNow.Ticks;

    private readonly ILogger _logger;

    private readonly IQuartzMongoDbJobStoreFactory _factory;
    private readonly IMongoDatabase _database;
    private readonly IServiceProvider _serviceProvider;

    private IQuartzJobStoreLockingManager _lockingManager = null!;

    private ISchedulerSignaler _schedulerSignaler = null!;

    private LockRepository _lockRepository = null!;
    private CalendarRepository _calendarRepository = null!;
    private FiredTriggerRepository _firedTriggerRepository = null!;
    private JobDetailRepository _jobDetailRepository = null!;
    private PausedTriggerGroupRepository _pausedTriggerGroupRepository = null!;
    private SchedulerRepository _schedulerRepository = null!;
    private TriggerRepository _triggerRepository = null!;

    private MisfireHandler? _misfireHandler;
    private TimeSpan _misfireThreshold = TimeSpan.FromMinutes(1);
    private ClusterManager? _clusterManager;

    private bool _schedulerRunning;
    private volatile bool _shutdown;

    private readonly SemaphoreSlim _pendingLocksSemaphore = new(1);

    public string? CollectionPrefix { get; set; }

    /// <summary>
    ///     Get or set the maximum number of misfired triggers that the misfire handling
    ///     thread will try to recover at one time (within one transaction).  The
    ///     default is 20.
    /// </summary>
    public int MaxMisfiresToHandleAtATime { get; set; } = 20;

    /// <summary>
    ///     Gets or sets the database retry interval.
    /// </summary>
    /// <value>The db retry interval.</value>
    [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
    public TimeSpan DbRetryInterval { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>
    ///     The time span by which a trigger must have missed its
    ///     next-fire-time, in order for it to be considered "misfired" and thus
    ///     have its misfire instruction applied.
    /// </summary>
    [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
    public TimeSpan MisfireThreshold
    {
        get => _misfireThreshold;
        set
        {
            if (value.TotalMilliseconds < 1)
            {
                throw new ArgumentException("MisfireThreshold must be larger than 0");
            }

            _misfireThreshold = value;
        }
    }

    /// <summary>
    ///     Gets or sets the number of retries before an error is logged for recovery operations.
    /// </summary>
    public int RetryableActionErrorLogThreshold { get; set; } = 4;

    /// <summary>
    /// Get whether the threads spawned by this JobStore should be
    /// marked as daemon.
    /// </summary>
    /// <returns></returns>
    public bool MakeThreadsDaemons { get; set; }

    /// <summary>
    /// Get or set the frequency at which this instance "checks-in"
    /// with the other instances of the cluster. -- Affects the rate of
    /// detecting failed instances.
    /// </summary>
    [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
    public TimeSpan ClusterCheckinInterval { get; set; }

    /// <summary>
    /// The time span by which a check-in must have missed its
    /// next-fire-time, in order for it to be considered "misfired" and thus
    /// other scheduler instances in a cluster can consider a "misfired" scheduler
    /// instance as failed or dead.
    /// </summary>
    [TimeSpanParseRule(TimeSpanParseRule.Milliseconds)]
    public TimeSpan ClusterCheckinMisfireThreshold { get; set; }


    protected internal DateTimeOffset LastCheckin { get; set; } = SystemTime.UtcNow();

    protected bool _firstCheckIn = true;


    protected DateTimeOffset MisfireTime
    {
        get
        {
            var misfireTime = SystemTime.UtcNow();
            if (MisfireThreshold > TimeSpan.Zero)
            {
                misfireTime = misfireTime.AddMilliseconds(-1 * MisfireThreshold.TotalMilliseconds);
            }

            return misfireTime;
        }
    }

    public bool SupportsPersistence => true;
    public long EstimatedTimeToReleaseAndAcquireTrigger => 200;
    public bool Clustered { get; set; }


    /// <summary>
    /// Get or set the instance id of the Scheduler (must be unique within a cluster).
    /// </summary>
    public string InstanceId { get; set; } = string.Empty;

    /// <summary>
    /// Get or set the instance id of the Scheduler (must be unique within this server instance).
    /// </summary>
    public string InstanceName { get; set; } = string.Empty;

    public int ThreadPoolSize { get; set; }

    static MongoDbJobStore()
    {
        JobStoreClassMap.RegisterClassMaps();
    }

    public MongoDbJobStore(
        ILoggerFactory loggerFactory,
        IQuartzMongoDbJobStoreFactory factory,
        IServiceProvider serviceProvider
    )
    {
        LogProvider.SetLogProvider(loggerFactory);

        _logger = loggerFactory.CreateLogger<MongoDbJobStore>();

        ObjectSerializer.Initialize();

        _factory = factory;
        _database = _factory.GetDatabase();

        _serviceProvider = serviceProvider;
    }


    public async Task Initialize(
        ITypeLoadHelper loadHelper,
        ISchedulerSignaler signaler,
        CancellationToken token = default
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(InstanceId);
        ArgumentException.ThrowIfNullOrWhiteSpace(InstanceName);

        _schedulerSignaler = signaler;

        _logger.LogTrace("Scheduler {InstanceId}/{InstanceName} initialize", InstanceId, InstanceName);

        _calendarRepository = new CalendarRepository(_database, InstanceName, CollectionPrefix);
        _firedTriggerRepository = new FiredTriggerRepository(_database, InstanceName, CollectionPrefix);
        _jobDetailRepository = new JobDetailRepository(_database, InstanceName, CollectionPrefix);
        _lockRepository = new LockRepository(_database, InstanceName, CollectionPrefix);
        _pausedTriggerGroupRepository = new PausedTriggerGroupRepository(_database, InstanceName, CollectionPrefix);
        _schedulerRepository = new SchedulerRepository(_database, InstanceName, CollectionPrefix);
        _triggerRepository = new TriggerRepository(_database, InstanceName, CollectionPrefix);

        // Try to resolve the locking manager, if none has been registered - default to the mongodb tx one.
        _lockingManager = _serviceProvider.GetService<IQuartzJobStoreLockingManager>() ??
                          new MongoDbLockingManager(
                              _database.Client,
                              _lockRepository,
                              _serviceProvider.GetRequiredService<ILogger<MongoDbLockingManager>>()
                          );

        _logger.LogTrace("Validating quartz-store indices...");
        var repositories = new List<IRepository>
        {
            _schedulerRepository,
            _jobDetailRepository,
            _triggerRepository,
            _pausedTriggerGroupRepository,
            _firedTriggerRepository,
            _calendarRepository,
            _lockRepository,
        };

        foreach (var repository in repositories)
        {
            await repository.EnsureIndex();
        }
    }


    public async Task Shutdown(CancellationToken token = default)
    {
        _logger.LogTrace("Scheduler {InstanceId}/{InstanceName} shutdown", InstanceId, InstanceName);

        _shutdown = true;

        if (_misfireHandler != null)
        {
            await _misfireHandler.Shutdown().ConfigureAwait(false);
        }

        if (_clusterManager != null)
        {
            await _clusterManager.Shutdown().ConfigureAwait(false);
        }
    }


    public async Task StoreJobAndTrigger(
        IJobDetail newJob,
        IOperableTrigger newTrigger,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            await ExecuteInTx(
                    QuartzLockType.TriggerAccess,
                    async () =>
                    {
                        await StoreJobInternal(newJob, false).ConfigureAwait(false);

                        await StoreTriggerInternal(newTrigger, newJob, false, Models.TriggerState.Waiting, false, false)
                            .ConfigureAwait(false);
                    },
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (AggregateException ex)
        {
            throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    public Task<bool> IsJobGroupPaused(string groupName, CancellationToken token = default)
    {
        // This is not implemented in the core ADO stuff, so we won't implement it here either
        throw new NotSupportedException();
    }

    public Task<bool> IsTriggerGroupPaused(string groupName, CancellationToken token = default)
    {
        // This is not implemented in the core ADO stuff, so we won't implement it here either
        throw new NotSupportedException();
    }


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

    public Task StoreJobsAndTriggers(
        IReadOnlyDictionary<IJobDetail, IReadOnlyCollection<ITrigger>> triggersAndJobs,
        bool replace,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    foreach (var (job, triggers) in triggersAndJobs)
                    {
                        await StoreJobInternal(job, replace).ConfigureAwait(false);

                        foreach (var trigger in triggers)
                        {
                            await StoreTriggerInternal(
                                    (IOperableTrigger)trigger,
                                    job,
                                    replace,
                                    Models.TriggerState.Waiting,
                                    false,
                                    false
                                )
                                .ConfigureAwait(false);
                        }
                    }
                },
                cancellationToken
            );
        }
        catch (AggregateException ex)
        {
            throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
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
                        result = result && await RemoveJobInternal(jobKey).ConfigureAwait(false);
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
        var result = await _jobDetailRepository.GetJob(jobKey).ConfigureAwait(false);

        return result?.GetJobDetail();
    }


    public async Task<bool> CheckExists(JobKey jobKey, CancellationToken token = default)
    {
        return await _jobDetailRepository.JobExists(jobKey).ConfigureAwait(false);
    }

    public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken token = default)
    {
        return await _triggerRepository.TriggerExists(triggerKey).ConfigureAwait(false);
    }

    public Task ClearAllSchedulingData(CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    await _calendarRepository.DeleteAll().ConfigureAwait(false);
                    await _firedTriggerRepository.DeleteAll().ConfigureAwait(false);
                    await _jobDetailRepository.DeleteAll().ConfigureAwait(false);
                    await _pausedTriggerGroupRepository.DeleteAll().ConfigureAwait(false);
                    await _schedulerRepository.DeleteAll().ConfigureAwait(false);
                    await _triggerRepository.DeleteAll().ConfigureAwait(false);
                },
                token
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }


    public async Task<int> GetNumberOfJobs(CancellationToken token = default)
    {
        return (int)await _jobDetailRepository.GetCount().ConfigureAwait(false);
    }

    public async Task<int> GetNumberOfTriggers(CancellationToken token = default)
    {
        return (int)await _triggerRepository.GetCount().ConfigureAwait(false);
    }

    public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(
        GroupMatcher<JobKey> matcher,
        CancellationToken token = default
    )
    {
        var jobsKeys = await _jobDetailRepository.GetJobsKeys(matcher).ConfigureAwait(false);
        return new HashSet<JobKey>(jobsKeys);
    }


    public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken token = default)
    {
        return await _jobDetailRepository.GetJobGroupNames().ConfigureAwait(false);
    }


    public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken token = default)
    {
        return await _triggerRepository.GetTriggerGroupNames().ConfigureAwait(false);
    }


    public Task PauseJob(JobKey jobKey, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var triggers = await GetTriggersForJob(jobKey, token).ConfigureAwait(false);

                    foreach (var operableTrigger in triggers)
                    {
                        await PauseTriggerInternal(operableTrigger.Key).ConfigureAwait(false);
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
                    var jobKeys = await _jobDetailRepository.GetJobsKeys(matcher).ConfigureAwait(false);

                    var groupNames = new HashSet<string>();
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);

                        foreach (var trigger in triggers)
                        {
                            await PauseTriggerInternal(trigger.GetTriggerKey()).ConfigureAwait(false);
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

    public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
    {
        var groups = await _pausedTriggerGroupRepository.GetPausedTriggerGroups().ConfigureAwait(false);

        return new HashSet<string>(groups);
    }


    public Task ResumeJob(JobKey jobKey, CancellationToken cancellationToken = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var triggers = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);

                    await Task.WhenAll(triggers.Select(trigger => ResumeTriggerInternal(trigger.GetTriggerKey())))
                        .ConfigureAwait(false);
                },
                cancellationToken
            );
        }
        catch (AggregateException ex)
        {
            throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<IReadOnlyCollection<string>> ResumeJobs(
        GroupMatcher<JobKey> matcher,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            return ExecuteInTx<IReadOnlyCollection<string>>(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    var jobKeys = await _jobDetailRepository.GetJobsKeys(matcher).ConfigureAwait(false);
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = await _triggerRepository.GetTriggers(jobKey).ConfigureAwait(false);
                        await Task.WhenAll(triggers.Select(trigger => ResumeTriggerInternal(trigger.GetTrigger().Key)))
                            .ConfigureAwait(false);
                    }

                    return new HashSet<string>(jobKeys.Select(key => key.Group));
                },
                cancellationToken
            );
        }
        catch (AggregateException ex)
        {
            throw new JobPersistenceException(ex.InnerExceptions[0].Message, ex.InnerExceptions[0]);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task PauseAll(CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, PauseAllInternal, token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task ResumeAll(CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, ResumeAllInternal, token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    private async Task<IReadOnlyCollection<string>> PauseTriggerGroupInternal(GroupMatcher<TriggerKey> matcher)
    {
        await _triggerRepository.UpdateTriggersStates(
                matcher,
                Models.TriggerState.Paused,
                Models.TriggerState.Acquired,
                Models.TriggerState.Waiting
            )
            .ConfigureAwait(false);

        await _triggerRepository.UpdateTriggersStates(
                matcher,
                Models.TriggerState.PausedBlocked,
                Models.TriggerState.Blocked
            )
            .ConfigureAwait(false);

        var triggerGroups = await _triggerRepository.GetTriggerGroupNames(matcher).ConfigureAwait(false);

        // make sure to account for an exact group match for a group that doesn't yet exist
        var op = matcher.CompareWithOperator;
        if (op.Equals(StringOperator.Equality) && !triggerGroups.Contains(matcher.CompareToValue))
        {
            triggerGroups.Add(matcher.CompareToValue);
        }

        foreach (var triggerGroup in triggerGroups)
        {
            if (!await _pausedTriggerGroupRepository.IsTriggerGroupPaused(triggerGroup).ConfigureAwait(false))
            {
                await _pausedTriggerGroupRepository.AddPausedTriggerGroup(triggerGroup).ConfigureAwait(false);
            }
        }

        return new HashSet<string>(triggerGroups);
    }

    private async Task PauseAllInternal()
    {
        var groupNames = await _triggerRepository.GetTriggerGroupNames().ConfigureAwait(false);

        await Task.WhenAll(
                groupNames.Select(groupName =>
                    PauseTriggerGroupInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName))
                )
            )
            .ConfigureAwait(false);

        if (!await _pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused).ConfigureAwait(false))
        {
            await _pausedTriggerGroupRepository.AddPausedTriggerGroup(AllGroupsPaused).ConfigureAwait(false);
        }
    }

    private async Task<bool> ReplaceTriggerInternal(TriggerKey triggerKey, IOperableTrigger newTrigger)
    {
        // SELECT
        //  J.JOB_NAME,
        //  J.JOB_GROUP,
        //  J.IS_DURABLE,
        //  J.JOB_CLASS_NAME,
        //  J.REQUESTS_RECOVERY
        // FROM
        //  TRIGGERS T,
        //  JOB_DETAILS J
        // WHERE
        //  T.SCHED_NAME = @schedulerName AND
        //  T.SCHED_NAME = J.SCHED_NAME AND
        //  T.TRIGGER_NAME = @triggerName AND
        //  T.TRIGGER_GROUP = @triggerGroup AND
        //  T.JOB_NAME = J.JOB_NAME AND
        //  T.JOB_GROUP = J.JOB_GROUP";


        var trigger = await _triggerRepository.GetTrigger(triggerKey).ConfigureAwait(false);
        if (trigger == null)
        {
            return false;
        }

        var result = await _jobDetailRepository.GetJob(trigger.JobKey).ConfigureAwait(false);
        var job = result?.GetJobDetail();

        if (job == null)
        {
            return false;
        }

        if (!newTrigger.JobKey.Equals(job.Key))
        {
            throw new JobPersistenceException("New trigger is not related to the same job as the old trigger.");
        }

        var removedTrigger = await _triggerRepository.DeleteTrigger(triggerKey).ConfigureAwait(false);
        await StoreTriggerInternal(newTrigger, job, false, Models.TriggerState.Waiting, false, false)
            .ConfigureAwait(false);
        return removedTrigger > 0;
    }

    private async Task<bool> RemoveJobInternal(JobKey jobKey)
    {
        await _triggerRepository.DeleteTriggers(jobKey).ConfigureAwait(false);

        var result = await _jobDetailRepository.DeleteJob(jobKey).ConfigureAwait(false);
        return result > 0;
    }

    private async Task ResumeAllInternal()
    {
        var groupNames = await _triggerRepository.GetTriggerGroupNames().ConfigureAwait(false);

        await Task.WhenAll(
                groupNames.Select(groupName => ResumeTriggersInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))
            )
            .ConfigureAwait(false);

        await _pausedTriggerGroupRepository.DeletePausedTriggerGroup(AllGroupsPaused).ConfigureAwait(false);
    }

    private async Task StoreJobInternal(IJobDetail newJob, bool replaceExisting)
    {
        var existingJob = await _jobDetailRepository.JobExists(newJob.Key).ConfigureAwait(false);

        var jobDetail = new JobDetail(newJob, InstanceName);

        if (existingJob)
        {
            if (!replaceExisting)
            {
                throw new ObjectAlreadyExistsException(newJob);
            }

            await _jobDetailRepository.UpdateJob(jobDetail).ConfigureAwait(false);
        }
        else
        {
            await _jobDetailRepository.AddJob(jobDetail).ConfigureAwait(false);
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
                    if (!trigger.GetNextFireTimeUtc().HasValue)
                    {
                        // double check for possible reschedule within job
                        // execution, which would cancel the need to delete...
                        var trig = await _triggerRepository.GetTrigger(trigger.Key).ConfigureAwait(false);
                        if (trig != null && !trig.NextFireTime.HasValue)
                        {
                            await RemoveTriggerInternal(trigger.Key, jobDetail).ConfigureAwait(false);
                        }
                    }
                    else
                    {
                        await RemoveTriggerInternal(trigger.Key, jobDetail).ConfigureAwait(false);
                        SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    }

                    break;
                }
                case SchedulerInstruction.SetTriggerComplete:
                {
                    await _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Complete)
                        .ConfigureAwait(false);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
                case SchedulerInstruction.SetTriggerError:
                {
                    _logger.LogInformation("Trigger {Key} set to ERROR state.", trigger.Key);
                    await _triggerRepository.UpdateTriggerState(trigger.Key, Models.TriggerState.Error)
                        .ConfigureAwait(false);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
                case SchedulerInstruction.SetAllJobTriggersComplete:
                {
                    await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Complete)
                        .ConfigureAwait(false);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
                case SchedulerInstruction.SetAllJobTriggersError:
                {
                    _logger.LogInformation("All triggers of Job {JobKey} set to ERROR state.", trigger.JobKey);
                    await _triggerRepository.UpdateTriggersStates(trigger.JobKey, Models.TriggerState.Error)
                        .ConfigureAwait(false);
                    SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
                    break;
                }
            }

            if (jobDetail.ConcurrentExecutionDisallowed)
            {
                await _triggerRepository.UpdateTriggersStates(
                        jobDetail.Key,
                        Models.TriggerState.Waiting,
                        Models.TriggerState.Blocked
                    )
                    .ConfigureAwait(false);

                await _triggerRepository.UpdateTriggersStates(
                        jobDetail.Key,
                        Models.TriggerState.Paused,
                        Models.TriggerState.PausedBlocked
                    )
                    .ConfigureAwait(false);
                SignalSchedulingChangeOnTxCompletion(SchedulingSignalDateTime);
            }

            if (jobDetail.PersistJobDataAfterExecution && jobDetail.JobDataMap.Dirty)
            {
                await _jobDetailRepository.UpdateJobData(jobDetail.Key, jobDetail.JobDataMap).ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }

        try
        {
            await _firedTriggerRepository.DeleteFiredTrigger(trigger.FireInstanceId).ConfigureAwait(false);
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

    protected virtual DateTimeOffset? ClearAndGetSignalSchedulingChangeOnTxCompletion()
    {
        var t = LogicalThreadContext.GetData<DateTimeOffset?>(KeySignalChangeForTxCompletion);
        LogicalThreadContext.FreeNamedDataSlot(KeySignalChangeForTxCompletion);
        return t;
    }

    internal virtual void SignalSchedulingChangeImmediately(DateTimeOffset? candidateNewNextFireTime)
    {
        _schedulerSignaler.SignalSchedulingChange(candidateNewNextFireTime);
    }

    private void LogWarnIfNonZero(int val, [StructuredMessageTemplate] string? message, params object?[] args)
    {
        if (val > 0)
        {
            _logger.LogInformation(message, args);
        }
        else
        {
            _logger.LogDebug(message, args);
        }
    }


    #region Locking

    private async Task ExecuteInTx(
        QuartzLockType lockType,
        Func<Task> txCallback,
        CancellationToken cancellationToken = default
    )
    {
        await ExecuteInTx<object?>(
                lockType,
                async () =>
                {
                    await txCallback.Invoke().ConfigureAwait(false);
                    return null;
                },
                cancellationToken
            )
            .ConfigureAwait(false);
    }


    private async Task<T> ExecuteInTx<T>(
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            await _pendingLocksSemaphore.WaitAsync(cancellationToken).ConfigureAwait(false);

            return await _lockingManager.ExecuteTransaction(InstanceName, lockType, txCallback, cancellationToken)
                .ConfigureAwait(false);
        }
        finally
        {
            _pendingLocksSemaphore.Release();
        }
    }

    #endregion
}
