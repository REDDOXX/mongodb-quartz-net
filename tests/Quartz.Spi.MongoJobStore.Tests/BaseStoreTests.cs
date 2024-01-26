using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Quartz.Spi.MongoJobStore.Database;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Tests.Persistence;

namespace Quartz.Spi.MongoJobStore.Tests;

public abstract class BaseStoreTests
{
    public const string Barrier = "BARRIER";
    public const string DateStamps = "DATE_STAMPS";
    public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

    protected static async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
    {
        var sp = CreateServiceProvider();

        var schedulerFactory = sp.GetRequiredService<ISchedulerFactory>();
        return await schedulerFactory.GetScheduler();
    }

    private static ServiceProvider CreateServiceProvider()
    {
        var services = new ServiceCollection();
        services.AddLogging(
            builder =>
            {
                //
                builder.AddDebug();
            }
        );

        services.AddSingleton<IMongoDbJobStoreConnectionFactory, LocalMongoDbJobStoreConnectionFactory>();
        services.AddQuartz(
            q =>
            {
                q.SchedulerId = "AUTO";
                q.SchedulerName = "QUARTZ_TEST";

                q.UsePersistentStore<MongoDbJobStore>(
                    storage =>
                    {
                        storage.UseNewtonsoftJsonSerializer();

                        storage.ConfigureMongoDb(c => { c.CollectionPrefix = "HelloWorld"; });
                    }
                );
            }
        );

        return services.BuildServiceProvider();
    }
}
