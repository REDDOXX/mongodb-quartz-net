namespace Quartz.Spi.MongoJobStore.Tests.Jobs;

public class SimpleJob : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        throw new NotImplementedException();
    }
}
