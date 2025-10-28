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
            await _misfireHandler.Shutdown();
        }

        if (_clusterManager != null)
        {
            await _clusterManager.Shutdown();
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
                    await StoreJobInternal(newJob, false);

                    await StoreTriggerInternal(newTrigger, newJob, false, LocalTriggerState.Waiting, false, false);
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
                        await StoreJobInternal(job, replace);

                        foreach (var trigger in triggers)
                        {
                            await StoreTriggerInternal(
                                (IOperableTrigger)trigger,
                                job,
                                replace,
                                LocalTriggerState.Waiting,
                                false,
                                false
                            );
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


    public async Task<bool> CheckExists(JobKey jobKey, CancellationToken token = default)
    {
        return await _jobDetailRepository.JobExists(jobKey);
    }

    public async Task<bool> CheckExists(TriggerKey triggerKey, CancellationToken token = default)
    {
        return await _triggerRepository.TriggerExists(triggerKey);
    }

    public Task ClearAllSchedulingData(CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                async () =>
                {
                    await _calendarRepository.DeleteAll();
                    await _firedTriggerRepository.DeleteAll();
                    await _jobDetailRepository.DeleteAll();
                    await _pausedTriggerGroupRepository.DeleteAll();
                    await _schedulerRepository.DeleteAll();
                    await _triggerRepository.DeleteAll();
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
        return (int)await _jobDetailRepository.GetCount();
    }

    public async Task<int> GetNumberOfTriggers(CancellationToken token = default)
    {
        return (int)await _triggerRepository.GetCount();
    }

    public async Task<IReadOnlyCollection<JobKey>> GetJobKeys(
        GroupMatcher<JobKey> matcher,
        CancellationToken token = default
    )
    {
        var jobsKeys = await _jobDetailRepository.GetJobsKeys(matcher);
        return new HashSet<JobKey>(jobsKeys);
    }


    public async Task<IReadOnlyCollection<string>> GetJobGroupNames(CancellationToken token = default)
    {
        return await _jobDetailRepository.GetJobGroupNames();
    }

    public async Task<IReadOnlyCollection<string>> GetTriggerGroupNames(CancellationToken token = default)
    {
        return await _triggerRepository.GetTriggerGroupNames();
    }


    public async Task<IReadOnlyCollection<string>> GetPausedTriggerGroups(CancellationToken cancellationToken = default)
    {
        var groups = await _pausedTriggerGroupRepository.GetPausedTriggerGroups();

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
                    var triggers = await _triggerRepository.GetTriggers(jobKey);

                    await Task.WhenAll(triggers.Select(trigger => ResumeTriggerInternal(trigger.GetTriggerKey())));
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
                    var jobKeys = await _jobDetailRepository.GetJobsKeys(matcher);
                    foreach (var jobKey in jobKeys)
                    {
                        var triggers = await _triggerRepository.GetTriggers(jobKey);
                        await Task.WhenAll(
                            triggers.Select(trigger => ResumeTriggerInternal(
                                    trigger.GetTrigger()
                                           .Key
                                )
                            )
                        );
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
            LocalTriggerState.Paused,
            LocalTriggerState.Acquired,
            LocalTriggerState.Waiting
        );

        await _triggerRepository.UpdateTriggersStates(
            matcher,
            LocalTriggerState.PausedBlocked,
            LocalTriggerState.Blocked
        );

        var triggerGroups = await _triggerRepository.GetTriggerGroupNames(matcher);

        // make sure to account for an exact group match for a group that doesn't yet exist
        var op = matcher.CompareWithOperator;
        if (op.Equals(StringOperator.Equality) && !triggerGroups.Contains(matcher.CompareToValue))
        {
            triggerGroups.Add(matcher.CompareToValue);
        }

        foreach (var triggerGroup in triggerGroups)
        {
            if (!await _pausedTriggerGroupRepository.IsTriggerGroupPaused(triggerGroup))
            {
                await _pausedTriggerGroupRepository.AddPausedTriggerGroup(triggerGroup);
            }
        }

        return new HashSet<string>(triggerGroups);
    }

    private async Task PauseAllInternal()
    {
        var groupNames = await _triggerRepository.GetTriggerGroupNames();

        await Task.WhenAll(
            groupNames.Select(groupName => PauseTriggerGroupInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))
        );

        if (!await _pausedTriggerGroupRepository.IsTriggerGroupPaused(AllGroupsPaused))
        {
            await _pausedTriggerGroupRepository.AddPausedTriggerGroup(AllGroupsPaused);
        }
    }


    private async Task ResumeAllInternal()
    {
        var groupNames = await _triggerRepository.GetTriggerGroupNames();

        await Task.WhenAll(
            groupNames.Select(groupName => ResumeTriggersInternal(GroupMatcher<TriggerKey>.GroupEquals(groupName)))
        );

        await _pausedTriggerGroupRepository.DeletePausedTriggerGroup(AllGroupsPaused);
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
                await txCallback.Invoke();
                return null;
            },
            cancellationToken
        );
    }


    private async Task<T> ExecuteInTx<T>(
        QuartzLockType lockType,
        Func<Task<T>> txCallback,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            await _pendingLocksSemaphore.WaitAsync(cancellationToken);

            return await _lockingManager.ExecuteTransaction(InstanceName, lockType, txCallback, cancellationToken);
        }
        finally
        {
            _pendingLocksSemaphore.Release();
        }
    }

    #endregion
}
