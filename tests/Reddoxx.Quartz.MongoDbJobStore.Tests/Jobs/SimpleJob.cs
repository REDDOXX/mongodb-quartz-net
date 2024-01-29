using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.Jobs;

public class SimpleJob : IJob
{
    public Task Execute(IJobExecutionContext context)
    {
        throw new NotImplementedException();
    }
}
