using System.Diagnostics;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Quartz;
using Quartz.Spi.MongoJobStore.Database;

using Xunit;

namespace Quartz.Spi.MongoJobStore.Tests.DependencyInjection;

public class DependencyInjectionTest
{
    [Fact]
    public async Task SetupDependencyInjection()
    {
        var configBuilder = new ConfigurationBuilder();
        var config = configBuilder.Build();


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
                q.SchedulerName = "<InstanceName>";

                q.InterruptJobsOnShutdown = true;

                q.UsePersistentStore<MongoDbJobStore>(
                    storage =>
                    {
                        storage.UseNewtonsoftJsonSerializer();

                        //
                        storage.UseClustering();
                    }
                );
            }
        );

        var sp = services.BuildServiceProvider();


        var schedulerFactor = sp.GetRequiredService<ISchedulerFactory>();
        var scheduler = await schedulerFactor.GetScheduler();

        Debugger.Break();
    }
}
