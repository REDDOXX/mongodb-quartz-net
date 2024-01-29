using Quartz;
using Quartz.Impl;
using Quartz.Impl.Triggers;

using Xunit;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests;

public class ClusteringTests : BaseStoreTests, IDisposable
{
    private readonly IScheduler _scheduler;

    public ClusteringTests()
    {
        _scheduler = CreateScheduler(clustered: true).Result;
        _scheduler.Clear().Wait();
    }

    public void Dispose()
    {
        _scheduler.Shutdown().Wait();
        GC.SuppressFinalize(this);
    }

    [Fact]
    public async Task TestSqlServerJobStore()
    {
        try
        {
            await _scheduler.Clear();

            for (var i = 0; i < 100000; ++i)
            {
                var trigger = new SimpleTriggerImpl(
                    $"calendarsTrigger_{i}",
                    "test",
                    SimpleTriggerImpl.RepeatIndefinitely,
                    TimeSpan.FromSeconds(1)
                );

                var jd = new JobDetailImpl($"testJob_{i}", "test", typeof(NoOpJob));
                await _scheduler.ScheduleJob(jd, trigger);
            }

            await _scheduler.Start();
            await Task.Delay(TimeSpan.FromSeconds(30));
        }
        finally
        {
            await _scheduler.Shutdown(false);
        }
    }
}
