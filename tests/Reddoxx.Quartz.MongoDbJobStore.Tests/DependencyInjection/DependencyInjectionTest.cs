using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Database;
using Reddoxx.Quartz.MongoDbJobStore.Extensions;
using Reddoxx.Quartz.MongoDbJobStore.Tests.Persistence;

using Xunit;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.DependencyInjection;

public class DependencyInjectionTest
{
    [Fact]
    public async Task SetupDependencyInjection()
    {
        var services = new ServiceCollection();
        services.AddLogging(builder => { builder.AddDebug(); });

        services.AddSingleton<IQuartzMongoDbJobStoreFactory, LocalQuartzMongoDbJobStoreFactory>();
        services.AddQuartz(
            q =>
            {
                q.SchedulerName = Guid.NewGuid().ToString("N");
                q.InterruptJobsOnShutdown = true;

                q.UsePersistentStore<MongoDbJobStore>(
                    storage =>
                    {
                        storage.UseClustering();
                        storage.UseSystemTextJsonSerializer();

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
