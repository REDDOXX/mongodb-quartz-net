using ServiceStack.Redis;

namespace Reddoxx.Quartz.MongoDbJobStore.ServiceStackRedis;

public class Class1
{
    public Class1(IRedisClientsManager manager)
    {
        using var s = manager.GetClient().AcquireLock("{SchedulerName}{LockType}");

        try
        {
            using var s1 = manager.GetClient().AcquireLock("", TimeSpan.FromSeconds(1));
        }
        catch (TimeoutException ex)
        {
        }
    }
}
