using Quartz;
using Quartz.Impl.Matchers;

using Reddoxx.Quartz.MongoDbJobStore.Tests.Jobs;


namespace Reddoxx.Quartz.MongoDbJobStore.Tests;

public class MongoDbJobStoreTests : BaseStoreTests, IDisposable
{
    private readonly IScheduler _scheduler;

    public MongoDbJobStoreTests()
    {
        _scheduler = CreateScheduler()
            .Result;
        _scheduler.Clear()
                  .Wait();
    }

    public void Dispose()
    {
        _scheduler.Shutdown()
                  .Wait();

        GC.SuppressFinalize(this);
    }

    [Test]
    public async Task AddJobTest()
    {
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("j1")
                            .StoreDurably()
                            .Build();

        await Assert.That(_scheduler.CheckExists(new JobKey("j1")))
                    .IsFalse();


        await _scheduler.AddJob(job, false);

        await Assert.That(_scheduler.CheckExists(new JobKey("j1")))
                    .IsTrue();
    }

    [Test]
    public async Task RetrieveJobTest()
    {
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("j1")
                            .StoreDurably()
                            .Build();
        await _scheduler.AddJob(job, false);

        await Assert.That(await _scheduler.GetJobDetail(new JobKey("j1")))
                    .IsNotNull();
    }

    [Test]
    public async Task AddTriggerTest()
    {
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("j1")
                            .StoreDurably()
                            .Build();

        var trigger = TriggerBuilder.Create()
                                    .WithIdentity("t1")
                                    .ForJob(job)
                                    .StartNow()
                                    .WithSimpleSchedule(x => x.RepeatForever()
                                                              .WithIntervalInSeconds(5)
                                    )
                                    .Build();

        await Assert.That(await _scheduler.CheckExists(new TriggerKey("t1")))
                    .IsFalse();

        await _scheduler.ScheduleJob(job, trigger);

        await Assert.That(await _scheduler.CheckExists(new TriggerKey("t1")))
                    .IsTrue();

        await Assert.That(await _scheduler.GetJobDetail(new JobKey("j1")))
                    .IsNotNull();

        await Assert.That(await _scheduler.GetTrigger(new TriggerKey("t1")))
                    .IsNotNull();
    }

    [Test]
    public async Task GroupsTest()
    {
        await CreateJobsAndTriggers();

        var jobGroups = await _scheduler.GetJobGroupNames();
        var triggerGroups = await _scheduler.GetTriggerGroupNames();

        await Assert.That(jobGroups.Count)
                    .IsEqualTo(2);
        await Assert.That(triggerGroups.Count)
                    .IsEqualTo(2);

        var jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
        var triggerKeys =
            await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));

        await Assert.That(jobKeys.Count)
                    .IsEqualTo(1);
        await Assert.That(triggerKeys.Count)
                    .IsEqualTo(1);

        jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
        triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

        await Assert.That(jobKeys.Count)
                    .IsEqualTo(2);
        await Assert.That(triggerKeys.Count)
                    .IsEqualTo(2);
    }

    [Test]
    public async Task TriggerStateTest()
    {
        await CreateJobsAndTriggers();

        await Assert.That(await _scheduler.GetTriggerState(new TriggerKey("t2", "g1")))
                    .IsEqualTo(TriggerState.Normal);


        await _scheduler.PauseTrigger(new TriggerKey("t2", "g1"));

        await Assert.That(await _scheduler.GetTriggerState(new TriggerKey("t2", "g1")))
                    .IsEqualTo(TriggerState.Paused);


        await _scheduler.ResumeTrigger(new TriggerKey("t2", "g1"));
        await Assert.That(await _scheduler.GetTriggerState(new TriggerKey("t2", "g1")))
                    .IsEqualTo(TriggerState.Normal);

        var pausedGroups = await _scheduler.GetPausedTriggerGroups();
        await Assert.That(pausedGroups)
                    .IsEmpty();

        await _scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));

        // test that adding a trigger to a paused group causes the new trigger to be paused also... 
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("j4", "g1")
                            .Build();

        var trigger = TriggerBuilder.Create()
                                    .WithIdentity("t4", "g1")
                                    .ForJob(job)
                                    .StartNow()
                                    .WithSimpleSchedule(x => x.RepeatForever()
                                                              .WithIntervalInSeconds(5)
                                    )
                                    .Build();

        await _scheduler.ScheduleJob(job, trigger);

        pausedGroups = await _scheduler.GetPausedTriggerGroups();
        await Assert.That(pausedGroups.Count)
                    .IsEqualTo(1);

        var s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));
        await Assert.That(s)
                    .IsEqualTo(TriggerState.Paused);

        s = await _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
        await Assert.That(s)
                    .IsEqualTo(TriggerState.Paused);

        await _scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals("g1"));
        s = await _scheduler.GetTriggerState(new TriggerKey("t2", "g1"));

        await Assert.That(s)
                    .IsEqualTo(TriggerState.Normal);

        s = await _scheduler.GetTriggerState(new TriggerKey("t4", "g1"));
        await Assert.That(s)
                    .IsEqualTo(TriggerState.Normal);

        pausedGroups = await _scheduler.GetPausedTriggerGroups();
        await Assert.That(pausedGroups)
                    .IsEmpty();
    }

    [Test]
    public async Task SchedulingTest()
    {
        await CreateJobsAndTriggers();

        await Assert.That(await _scheduler.UnscheduleJob(new TriggerKey("foasldfksajdflk")))
                    .IsFalse();


        await Assert.That(await _scheduler.UnscheduleJob(new TriggerKey("t3", "g1")))
                    .IsTrue();


        var jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("g1"));
        var triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("g1"));

        await Assert.That(jobKeys.Count)
                    .IsEqualTo(1);
        // job should have been deleted also, because it is non-durable
        await Assert.That(triggerKeys.Count)
                    .IsEqualTo(1);

        await Assert.That(await _scheduler.UnscheduleJob(new TriggerKey("t1")))
                    .IsTrue();

        jobKeys = await _scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(JobKey.DefaultGroup));
        triggerKeys = await _scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals(TriggerKey.DefaultGroup));


        await Assert.That(jobKeys.Count)
                    .IsEqualTo(1);

        // job should have been left in place, because it is non-durable
        await Assert.That(triggerKeys)
                    .IsEmpty();
    }

    [Test]
    public async Task SimpleReschedulingTest()
    {
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("job1", "group1")
                            .Build();
        var trigger1 = TriggerBuilder.Create()
                                     .ForJob(job)
                                     .WithIdentity("trigger1", "group1")
                                     .StartAt(DateTimeOffset.Now.AddSeconds(30))
                                     .Build();

        await _scheduler.ScheduleJob(job, trigger1);

        job = await _scheduler.GetJobDetail(job.Key);
        Assert.NotNull(job);

        var trigger2 = TriggerBuilder.Create()
                                     .ForJob(job)
                                     .WithIdentity("trigger1", "group1")
                                     .StartAt(DateTimeOffset.Now.AddSeconds(60))
                                     .Build();
        await _scheduler.RescheduleJob(trigger1.Key, trigger2);
        job = await _scheduler.GetJobDetail(job.Key);
        Assert.NotNull(job);
    }

    [Test]
    public async Task TestAbilityToFireImmediatelyWhenStartedBefore()
    {
        var jobExecTimestamps = new List<DateTime>();
        var barrier = new Barrier(2);

        _scheduler.Context.Put(Barrier, barrier);
        _scheduler.Context.Put(DateStamps, jobExecTimestamps);
        await _scheduler.Start();

        Thread.Yield();

        var job1 = JobBuilder.Create<SimpleJobWithSync>()
                             .WithIdentity("job1")
                             .Build();

        var trigger1 = TriggerBuilder.Create()
                                     .ForJob(job1)
                                     .Build();

        var sTime = DateTime.UtcNow;

        await _scheduler.ScheduleJob(job1, trigger1);

        barrier.SignalAndWait(TestTimeout);

        await _scheduler.Shutdown(false);

        var fTime = jobExecTimestamps[0];

        // Immediate trigger did not fire within a reasonable amount of time.
        await Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000))
                    .IsTrue();
    }

    [Test]
    public async Task TestAbilityToFireImmediatelyWhenStartedBeforeWithTriggerJob()
    {
        var jobExecTimestamps = new List<DateTime>();
        var barrier = new Barrier(2);

        await _scheduler.Clear();

        _scheduler.Context.Put(Barrier, barrier);
        _scheduler.Context.Put(DateStamps, jobExecTimestamps);

        await _scheduler.Start();

        Thread.Yield();

        var job1 = JobBuilder.Create<SimpleJobWithSync>()
                             .WithIdentity("job1")
                             .StoreDurably()
                             .Build();
        await _scheduler.AddJob(job1, false);

        var sTime = DateTime.UtcNow;

        await _scheduler.TriggerJob(job1.Key);

        barrier.SignalAndWait(TestTimeout);

        await _scheduler.Shutdown(false);

        var fTime = jobExecTimestamps[0];

        // Immediate trigger did not fire within a reasonable amount of time
        await Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000))
                    .IsTrue();
        // This is dangerously subjective!  but what else to do?
    }

    [Test]
    public async Task TestAbilityToFireImmediatelyWhenStartedAfter()
    {
        var jobExecTimestamps = new List<DateTime>();

        var barrier = new Barrier(2);

        _scheduler.Context.Put(Barrier, barrier);
        _scheduler.Context.Put(DateStamps, jobExecTimestamps);

        var job1 = JobBuilder.Create<SimpleJobWithSync>()
                             .WithIdentity("job1")
                             .Build();
        var trigger1 = TriggerBuilder.Create()
                                     .ForJob(job1)
                                     .Build();

        var sTime = DateTime.UtcNow;

        await _scheduler.ScheduleJob(job1, trigger1);
        await _scheduler.Start();

        barrier.SignalAndWait(TestTimeout);

        await _scheduler.Shutdown(false);

        var fTime = jobExecTimestamps[0];

        // Immediate trigger did not fire within a reasonable amount of time.
        await Assert.That(fTime - sTime < TimeSpan.FromMilliseconds(7000))
                    .IsTrue();
        // This is dangerously subjective!  but what else to do?
    }

    [Test]
    public async Task TestScheduleMultipleTriggersForAJob()
    {
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("job1", "group1")
                            .Build();
        var trigger1 = TriggerBuilder.Create()
                                     .WithIdentity("trigger1", "group1")
                                     .StartNow()
                                     .WithSimpleSchedule(x => x.WithIntervalInSeconds(1)
                                                               .RepeatForever()
                                     )
                                     .Build();
        var trigger2 = TriggerBuilder.Create()
                                     .WithIdentity("trigger2", "group1")
                                     .StartNow()
                                     .WithSimpleSchedule(x => x.WithIntervalInSeconds(1)
                                                               .RepeatForever()
                                     )
                                     .Build();

        var triggersForJob = new HashSet<ITrigger>
        {
            trigger1,
            trigger2,
        };

        await _scheduler.ScheduleJob(job, triggersForJob, true);

        var triggersOfJob = await _scheduler.GetTriggersOfJob(job.Key);
        await Assert.That(triggersOfJob.Count)
                    .IsEqualTo(2);


        await Assert.That(triggersOfJob)
                    .Contains(trigger1);

        await Assert.That(triggersOfJob)
                    .Contains(trigger2);

        await _scheduler.Shutdown(false);
    }

    [Test]
    public async Task TestDurableStorageFunctions()
    {
        // test basic storage functions of scheduler...

        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("j1")
                            .StoreDurably()
                            .Build();

        await Assert.That(await _scheduler.CheckExists(new JobKey("j1")))
                    .IsFalse();

        await _scheduler.AddJob(job, false);

        await Assert.That(await _scheduler.CheckExists(new JobKey("j1")))
                    .IsTrue();

        var nonDurableJob = JobBuilder.Create<SimpleJob>()
                                      .WithIdentity("j2")
                                      .Build();

        await Assert.ThrowsAsync(async () => await _scheduler.AddJob(nonDurableJob, false))
                    .WithExceptionType(typeof(SchedulerException));

        await Assert.That(await _scheduler.CheckExists(new JobKey("j2")))
                    .IsFalse();

        await _scheduler.AddJob(nonDurableJob, false, true);

        await Assert.That(await _scheduler.CheckExists(new JobKey("j2")))
                    .IsTrue();
    }

    [Test]
    public async Task TestShutdownWithoutWaitIsUnclean()
    {
        var jobExecTimestamps = new List<DateTime>();
        var barrier = new Barrier(2);
        try
        {
            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);
            await _scheduler.Start();

            var jobName = Guid.NewGuid()
                              .ToString();
            await _scheduler.AddJob(
                JobBuilder.Create<SimpleJobWithSync>()
                          .WithIdentity(jobName)
                          .StoreDurably()
                          .Build(),
                false
            );

            await _scheduler.ScheduleJob(
                TriggerBuilder.Create()
                              .ForJob(jobName)
                              .StartNow()
                              .Build()
            );
            while ((await _scheduler.GetCurrentlyExecutingJobs()).Count == 0)
            {
                await Task.Delay(50);
            }
        }
        finally
        {
            await _scheduler.Shutdown(false);
        }

        barrier.SignalAndWait(TestTimeout);
    }

    [Test]
    public async Task TestShutdownWithWaitIsClean()
    {
        var shutdown = false;
        var jobExecTimestamps = new List<DateTime>();
        var barrier = new Barrier(2);
        try
        {
            _scheduler.Context.Put(Barrier, barrier);
            _scheduler.Context.Put(DateStamps, jobExecTimestamps);
            await _scheduler.Start();
            var jobName = Guid.NewGuid()
                              .ToString();
            await _scheduler.AddJob(
                JobBuilder.Create<SimpleJobWithSync>()
                          .WithIdentity(jobName)
                          .StoreDurably()
                          .Build(),
                false
            );
            await _scheduler.ScheduleJob(
                TriggerBuilder.Create()
                              .ForJob(jobName)
                              .StartNow()
                              .Build()
            );
            while ((await _scheduler.GetCurrentlyExecutingJobs()).Count == 0)
            {
                await Task.Delay(50);
            }
        }
        catch
        {
            // Ignored
        }

        var task = Task.Run(async () =>
            {
                try
                {
                    await _scheduler.Shutdown(true);
                    shutdown = true;
                }
                catch (SchedulerException ex)
                {
                    throw new Exception("exception: " + ex.Message, ex);
                }
            }
        );

        await Task.Delay(1000);

        await Assert.That(shutdown)
                    .IsFalse();

        barrier.SignalAndWait(TestTimeout);
        await task;
    }

    [Test]
    public async Task SmokeTest()
    {
        await SmokeTestPerformer.Test(_scheduler, true, true);
    }


    private async Task CreateJobsAndTriggers()
    {
        var job = JobBuilder.Create<SimpleJob>()
                            .WithIdentity("j1")
                            .StoreDurably()
                            .Build();

        var trigger = TriggerBuilder.Create()
                                    .WithIdentity("t1")
                                    .ForJob(job)
                                    .StartNow()
                                    .WithSimpleSchedule(x => x.RepeatForever()
                                                              .WithIntervalInSeconds(5)
                                    )
                                    .Build();

        await _scheduler.ScheduleJob(job, trigger);

        job = JobBuilder.Create<SimpleJob>()
                        .WithIdentity("j2", "g1")
                        .Build();

        trigger = TriggerBuilder.Create()
                                .WithIdentity("t2", "g1")
                                .ForJob(job)
                                .StartNow()
                                .WithSimpleSchedule(x => x.RepeatForever()
                                                          .WithIntervalInSeconds(5)
                                )
                                .Build();

        await _scheduler.ScheduleJob(job, trigger);

        job = JobBuilder.Create<SimpleJob>()
                        .WithIdentity("j3", "g1")
                        .Build();

        trigger = TriggerBuilder.Create()
                                .WithIdentity("t3", "g1")
                                .ForJob(job)
                                .StartNow()
                                .WithSimpleSchedule(x => x.RepeatForever()
                                                          .WithIntervalInSeconds(5)
                                )
                                .Build();

        await _scheduler.ScheduleJob(job, trigger);
    }
}
