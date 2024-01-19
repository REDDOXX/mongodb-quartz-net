namespace Quartz.Spi.MongoJobStore.Tests.Jobs;

public class SimpleJobWithSync : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        try
        {
            var jobExecTimestamps = (List<DateTime>)context.Scheduler.Context.Get(BaseStoreTests.DateStamps);
            var barrier = (Barrier)context.Scheduler.Context.Get(BaseStoreTests.Barrier);

            jobExecTimestamps.Add(DateTime.UtcNow);

            barrier.SignalAndWait(BaseStoreTests.TestTimeout);
        }
        catch (Exception e)
        {
            Console.Write(e);
            throw e;
        }

        return Task.FromResult(0);
    }
}
