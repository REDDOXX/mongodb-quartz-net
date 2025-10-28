using Quartz;
using Quartz.Impl;
using Quartz.Impl.Calendar;
using Quartz.Impl.Matchers;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests;

public static class SmokeTestPerformer
{
    public static async Task Test(IScheduler scheduler, bool clearJobs, bool scheduleJobs)
    {
        try
        {
            if (clearJobs)
            {
                await scheduler.Clear();
            }

            if (scheduleJobs)
            {
                var cronCalendar = new CronCalendar("0/5 * * * * ?");
                var holidayCalendar = new HolidayCalendar();

                // QRTZNET-86
                var t = await scheduler.GetTrigger(new TriggerKey("NonExistingTrigger", "NonExistingGroup"));
                await Assert.That(t)
                            .IsNull();

                var cal = new AnnualCalendar();
                await scheduler.AddCalendar("annualCalendar", cal, false, true);

                var calendarsTrigger = new SimpleTriggerImpl(
                    "calendarsTrigger",
                    "test",
                    20,
                    TimeSpan.FromMilliseconds(5)
                )
                {
                    CalendarName = "annualCalendar",
                };

                var jd = new JobDetailImpl("testJob", "test", typeof(NoOpJob));
                await scheduler.ScheduleJob(jd, calendarsTrigger);

                // QRTZNET-93
                await scheduler.AddCalendar("annualCalendar", cal, true, true);

                await scheduler.AddCalendar("baseCalendar", new BaseCalendar(), false, true);
                await scheduler.AddCalendar("cronCalendar", cronCalendar, false, true);
                await scheduler.AddCalendar(
                    "dailyCalendar",
                    new DailyCalendar(DateTime.Now.Date, DateTime.Now.AddMinutes(1)),
                    false,
                    true
                );
                await scheduler.AddCalendar("holidayCalendar", holidayCalendar, false, true);
                await scheduler.AddCalendar("monthlyCalendar", new MonthlyCalendar(), false, true);
                await scheduler.AddCalendar("weeklyCalendar", new WeeklyCalendar(), false, true);

                await scheduler.AddCalendar("cronCalendar", cronCalendar, true, true);
                await scheduler.AddCalendar("holidayCalendar", holidayCalendar, true, true);

                await Assert.That(await scheduler.GetCalendar("annualCalendar"))
                            .IsNotNull();

                var lonelyJob = new JobDetailImpl("lonelyJob", "lonelyGroup", typeof(SimpleRecoveryJob))
                {
                    Durable = true,
                    RequestsRecovery = true,
                };
                await scheduler.AddJob(lonelyJob, false);
                await scheduler.AddJob(lonelyJob, true);

                var schedId = scheduler.SchedulerInstanceId;

                var count = 1;

                var job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };

                var trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(5));
                trigger.JobDataMap.Add("key", "value");
                trigger.JobDataMap.Add(
                    Guid.NewGuid()
                        .ToString(),
                    Guid.NewGuid()
                );
                trigger.EndTimeUtc = DateTime.UtcNow.AddYears(10);

                trigger.StartTimeUtc = DateTime.Now.AddMilliseconds(1000L);
                await scheduler.ScheduleJob(job, trigger);

                // check that trigger was stored
                var persisted = await scheduler.GetTrigger(new TriggerKey("trig_" + count, schedId));
                await Assert.That(persisted)
                            .IsNotNull();
                await Assert.That(persisted is SimpleTriggerImpl)
                            .IsTrue();

                count++;
                job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };

                trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(5))
                {
                    StartTimeUtc = DateTime.Now.AddMilliseconds(2000L),
                };
                await scheduler.ScheduleJob(job, trigger);

                count++;
                job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryStatefulJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };

                trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(3))
                {
                    StartTimeUtc = DateTime.Now.AddMilliseconds(1000L),
                };

                await scheduler.ScheduleJob(job, trigger);

                count++;
                job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };
                trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromSeconds(4))
                {
                    StartTimeUtc = DateTime.Now.AddMilliseconds(1000L),
                };
                await scheduler.ScheduleJob(job, trigger);

                count++;
                job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };
                trigger = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromMilliseconds(4500));
                await scheduler.ScheduleJob(job, trigger);

                count++;
                job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };
                IOperableTrigger ct = new CronTriggerImpl("cron_trig_" + count, schedId, "0/10 * * * * ?");
                ct.JobDataMap.Add("key", "value");
                ct.StartTimeUtc = DateTime.Now.AddMilliseconds(1000);

                await scheduler.ScheduleJob(job, ct);

                count++;
                job = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob))
                {
                    // ask scheduler to re-Execute this job if it was in progress when
                    // the scheduler went down...
                    RequestsRecovery = true,
                };
                var nt = new DailyTimeIntervalTriggerImpl(
                    "nth_trig_" + count,
                    schedId,
                    new TimeOfDay(1, 1, 1),
                    new TimeOfDay(23, 30, 0),
                    IntervalUnit.Hour,
                    1
                )
                {
                    StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000),
                };

                await scheduler.ScheduleJob(job, nt);

                var nt2 = new DailyTimeIntervalTriggerImpl
                {
                    Key = new TriggerKey("nth_trig2_" + count, schedId),
                    StartTimeUtc = DateTime.Now.Date.AddMilliseconds(1000),
                    JobKey = job.Key,
                };
                await scheduler.ScheduleJob(nt2);

                // GitHub issue #92
                await scheduler.GetTrigger(nt2.Key);

                // GitHub issue #98
                nt2.StartTimeOfDay = new TimeOfDay(1, 2, 3);
                nt2.EndTimeOfDay = new TimeOfDay(2, 3, 4);

                await scheduler.UnscheduleJob(nt2.Key);
                await scheduler.ScheduleJob(nt2);

                var triggerFromDb = await scheduler.GetTrigger(nt2.Key) as IDailyTimeIntervalTrigger;
                Assert.NotNull(triggerFromDb);

                await Assert.That(triggerFromDb.StartTimeOfDay.Hour)
                            .IsEqualTo(1);
                await Assert.That(triggerFromDb.StartTimeOfDay.Minute)
                            .IsEqualTo(2);
                await Assert.That(triggerFromDb.StartTimeOfDay.Second)
                            .IsEqualTo(3);

                await Assert.That(triggerFromDb.EndTimeOfDay.Hour)
                            .IsEqualTo(2);
                await Assert.That(triggerFromDb.EndTimeOfDay.Minute)
                            .IsEqualTo(3);
                await Assert.That(triggerFromDb.EndTimeOfDay.Second)
                            .IsEqualTo(4);

                job.RequestsRecovery = true;
                var intervalTrigger = new CalendarIntervalTriggerImpl(
                    "calint_trig_" + count,
                    schedId,
                    DateTime.UtcNow.AddMilliseconds(300),
                    DateTime.UtcNow.AddMinutes(1),
                    IntervalUnit.Second,
                    8
                )
                {
                    JobKey = job.Key,
                };

                await scheduler.ScheduleJob(intervalTrigger);

                // bulk operations
                var detail = new JobDetailImpl("job_" + count, schedId, typeof(SimpleRecoveryJob));
                var simple = new SimpleTriggerImpl("trig_" + count, schedId, 20, TimeSpan.FromMilliseconds(4500));

                var triggers = new HashSet<ITrigger>
                {
                    simple,
                };
                var info = new Dictionary<IJobDetail, IReadOnlyCollection<ITrigger>>
                {
                    [detail] = triggers,
                };

                await scheduler.ScheduleJobs(info, true);

                await Assert.That(await scheduler.CheckExists(detail.Key))
                            .IsTrue();
                await Assert.That(await scheduler.CheckExists(simple.Key))
                            .IsTrue();

                // QRTZNET-243
                await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("a"));
                await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("a"));
                await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("a"));
                await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("a"));

                await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("a"));
                await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("a"));
                await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("a"));
                await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("a"));

                await scheduler.Start();

                await Task.Delay(TimeSpan.FromSeconds(3));

                await scheduler.PauseAll();

                await scheduler.ResumeAll();

                await scheduler.PauseJob(new JobKey("job_1", schedId));

                await scheduler.ResumeJob(new JobKey("job_1", schedId));

                await scheduler.PauseJobs(GroupMatcher<JobKey>.GroupEquals(schedId));

                await Task.Delay(TimeSpan.FromSeconds(1));

                await scheduler.ResumeJobs(GroupMatcher<JobKey>.GroupEquals(schedId));

                await scheduler.PauseTrigger(new TriggerKey("trig_2", schedId));
                await scheduler.ResumeTrigger(new TriggerKey("trig_2", schedId));

                await scheduler.PauseTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId));

                await Assert.That((await scheduler.GetPausedTriggerGroups()).Count)
                            .IsEqualTo(1);

                await Task.Delay(TimeSpan.FromSeconds(3));
                await scheduler.ResumeTriggers(GroupMatcher<TriggerKey>.GroupEquals(schedId));

                await Assert.That(await scheduler.GetTrigger(new TriggerKey("trig_2", schedId)))
                            .IsNotNull();
                await Assert.That(await scheduler.GetJobDetail(new JobKey("job_1", schedId)))
                            .IsNotNull();
                await Assert.That(await scheduler.GetMetaData())
                            .IsNotNull();
                await Assert.That(await scheduler.GetCalendar("weeklyCalendar"))
                            .IsNotNull();

                var genericJobKey = new JobKey("genericJob", "genericGroup");
                var genericJob = JobBuilder.Create<GenericJobType>()
                                           .WithIdentity(genericJobKey)
                                           .WithDescription("HelloWorld Test")
                                           .StoreDurably()
                                           .Build();

                await scheduler.AddJob(genericJob, false);

                genericJob = await scheduler.GetJobDetail(genericJobKey);
                await Assert.That(genericJob)
                            .IsNotNull();
                await scheduler.TriggerJob(genericJobKey);

                await Task.Delay(TimeSpan.FromSeconds(60));

                await Assert.That(GenericJobType.TriggeredCount)
                            .IsEqualTo(1);
                await scheduler.Standby();

                await Assert.That(await scheduler.GetCalendarNames())
                            .IsNotEmpty();
                await Assert.That(await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals(schedId)))
                            .IsNotNull();
                await Assert.That(await scheduler.GetTriggersOfJob(new JobKey("job_2", schedId)))
                            .IsNotNull();
                await Assert.That(await scheduler.GetJobDetail(new JobKey("job_2", schedId)))
                            .IsNotNull();

                await scheduler.DeleteCalendar("cronCalendar");
                await scheduler.DeleteCalendar("holidayCalendar");
                await scheduler.DeleteJob(new JobKey("lonelyJob", "lonelyGroup"));
                await scheduler.DeleteJob(job.Key);

                await scheduler.GetJobGroupNames();
                await scheduler.GetCalendarNames();
                await scheduler.GetTriggerGroupNames();

                await TestMatchers(scheduler);
            }
        }
        finally
        {
            await scheduler.Shutdown(false);
        }
    }

    private static async Task TestMatchers(IScheduler scheduler)
    {
        await scheduler.Clear();

        var job = JobBuilder.Create<NoOpJob>()
                            .WithIdentity("job1", "aaabbbccc")
                            .StoreDurably()
                            .Build();
        await scheduler.AddJob(job, true);
        var schedule = SimpleScheduleBuilder.Create();
        var trigger = TriggerBuilder.Create()
                                    .WithIdentity("trig1", "aaabbbccc")
                                    .WithSchedule(schedule)
                                    .ForJob(job)
                                    .Build();
        await scheduler.ScheduleJob(trigger);

        job = JobBuilder.Create<NoOpJob>()
                        .WithIdentity("job1", "xxxyyyzzz")
                        .StoreDurably()
                        .Build();
        await scheduler.AddJob(job, true);
        schedule = SimpleScheduleBuilder.Create();
        trigger = TriggerBuilder.Create()
                                .WithIdentity("trig1", "xxxyyyzzz")
                                .WithSchedule(schedule)
                                .ForJob(job)
                                .Build();
        await scheduler.ScheduleJob(trigger);

        job = JobBuilder.Create<NoOpJob>()
                        .WithIdentity("job2", "xxxyyyzzz")
                        .StoreDurably()
                        .Build();
        await scheduler.AddJob(job, true);
        schedule = SimpleScheduleBuilder.Create();
        trigger = TriggerBuilder.Create()
                                .WithIdentity("trig2", "xxxyyyzzz")
                                .WithSchedule(schedule)
                                .ForJob(job)
                                .Build();
        await scheduler.ScheduleJob(trigger);

        var jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.AnyGroup());
        await Assert.That(jkeys.Count)
                    .IsEqualTo(3);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("xxxyyyzzz"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(2);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEquals("aaabbbccc"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(1);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("aa"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(1);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupStartsWith("xx"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(2);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("cc"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(1);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupEndsWith("zzz"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(2);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("bc"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(1);

        jkeys = await scheduler.GetJobKeys(GroupMatcher<JobKey>.GroupContains("yz"));
        await Assert.That(jkeys.Count)
                    .IsEqualTo(2);

        var tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.AnyGroup());
        await Assert.That(tkeys.Count)
                    .IsEqualTo(3);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("xxxyyyzzz"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(2);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEquals("aaabbbccc"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(1);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("aa"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(1);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupStartsWith("xx"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(2);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("cc"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(1);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupEndsWith("zzz"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(2);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("bc"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(1);

        tkeys = await scheduler.GetTriggerKeys(GroupMatcher<TriggerKey>.GroupContains("yz"));
        await Assert.That(tkeys.Count)
                    .IsEqualTo(2);
    }
}

public class NoOpJob : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        return Task.FromResult(0);
    }
}

public class GenericJobType : IJob
{
    public static int TriggeredCount { get; private set; }

    public Task Execute(IJobExecutionContext context)
    {
        TriggeredCount++;
        return Task.FromResult(0);
    }
}

public class SimpleRecoveryJob : IJob
{
    private const string Count = "count";

    /// <summary>
    ///     Called by the <see cref="IScheduler" /> when a
    ///     <see cref="ITrigger" /> fires that is associated with
    ///     the <see cref="IJob" />.
    /// </summary>
    public virtual async Task Execute(IJobExecutionContext context)
    {
        // delay for ten seconds
        try
        {
            await Task.Delay(TimeSpan.FromSeconds(10));
        }
        catch (ThreadInterruptedException)
        {
        }

        var data = context.JobDetail.JobDataMap;
        var count = data.ContainsKey(Count) ? data.GetInt(Count) : 0;

        count++;
        data.Put(Count, count);
    }
}

[DisallowConcurrentExecution]
[PersistJobDataAfterExecution]
public class SimpleRecoveryStatefulJob : SimpleRecoveryJob;
