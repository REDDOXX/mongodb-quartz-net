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

    protected static async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST", bool clustered = false)
    {
        var sp = CreateServiceProvider(instanceName, clustered);

        var schedulerFactory = sp.GetRequiredService<ISchedulerFactory>();
        return await schedulerFactory.GetScheduler();
    }

    private static ServiceProvider CreateServiceProvider(string instanceName, bool clustered)
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => { builder.AddDebug(); });

        services.AddSingleton<IMongoDbJobStoreConnectionFactory, LocalMongoDbJobStoreConnectionFactory>();
        services.AddQuartz(
            q =>
            {
                q.SchedulerId = "AUTO";
                q.SchedulerName = instanceName;
                q.MaxBatchSize = 10;

                q.UsePersistentStore<MongoDbJobStore>(
                    storage =>
                    {
                        if (clustered)
                        {
                            storage.UseClustering(
                                c =>
                                {
                                    c.CheckinInterval = TimeSpan.FromSeconds(10);
                                    c.CheckinMisfireThreshold = TimeSpan.FromSeconds(15);
                                }
                            );
                        }

                        storage.UseNewtonsoftJsonSerializer();

                        storage.ConfigureMongoDb(
                            c =>
                            {
                                //
                                c.CollectionPrefix = "HelloWorld";
                            }
                        );
                    }
                );
            }
        );

        return services.BuildServiceProvider();
    }
}
