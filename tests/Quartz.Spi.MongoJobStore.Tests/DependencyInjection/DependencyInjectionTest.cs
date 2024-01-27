using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Quartz.Spi.MongoJobStore.Database;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Tests.Persistence;

using Xunit;

namespace Quartz.Spi.MongoJobStore.Tests.DependencyInjection;

public class DependencyInjectionTest
{
    [Fact]
    public async Task SetupDependencyInjection()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => { builder.AddDebug(); });

        services.AddSingleton<IMongoDbJobStoreConnectionFactory, LocalMongoDbJobStoreConnectionFactory>();
        services.AddQuartz(
            q =>
            {
                q.SchedulerName = Guid.NewGuid().ToString("N");
                q.InterruptJobsOnShutdown = true;

                q.UsePersistentStore<MongoDbJobStore>(
                    storage =>
                    {
                        storage.UseClustering();
                        storage.UseNewtonsoftJsonSerializer();

                        storage.ConfigureMongoDb(
                            c =>
                            {
                                //
                                c.CollectionPrefix = Guid.NewGuid().ToString("N");
                            }
                        );
                    }
                );
            }
        );

        var sp = services.BuildServiceProvider();

        var schedulerFactor = sp.GetRequiredService<ISchedulerFactory>();
        var scheduler = await schedulerFactor.GetScheduler();

        Assert.NotNull(scheduler);
    }
}
