using System.Reflection;

using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Database;
using Reddoxx.Quartz.MongoDbJobStore.Extensions;
using Reddoxx.Quartz.MongoDbJobStore.Locking;
using Reddoxx.Quartz.MongoDbJobStore.Redlock;
using Reddoxx.Quartz.MongoDbJobStore.Tests.Options;
using Reddoxx.Quartz.MongoDbJobStore.Tests.Persistence;

using StackExchange.Redis;
using StackExchange.Redis.Extensions.Core.Configuration;
using StackExchange.Redis.Extensions.System.Text.Json;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests;

public abstract class BaseStoreTests
{
    public const string Barrier = "BARRIER";
    public const string DateStamps = "DATE_STAMPS";
    public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

    protected static async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST", bool clustered = true)
    {
        var sp = CreateServiceProvider(instanceName, clustered);

        var schedulerFactory = sp.GetRequiredService<ISchedulerFactory>();
        return await schedulerFactory.GetScheduler();
    }

    protected static ServiceProvider CreateServiceProvider(string instanceName, bool clustered)
    {
        var configuration = new ConfigurationBuilder()
            // 
            .AddUserSecrets(Assembly.GetExecutingAssembly(), true)
            .Build();


        var services = new ServiceCollection();
        services.AddLogging(builder => { builder.AddDebug(); });

        services.AddSingleton<IConfiguration>(configuration);

        services.AddOptions<MongoDbOptions>().Bind(configuration.GetSection("MongoDb"));
        services.AddOptions<RedisOptions>().Bind(configuration.GetSection("Redis"));


        services.AddStackExchangeRedisExtensions<SystemTextJsonSerializer>(
            sp =>
            {
                var options = sp.GetRequiredService<IOptions<RedisOptions>>().Value;

                return
                [
                    new RedisConfiguration
                    {
                        Hosts =
                        [
                            new RedisHost
                            {
                                Host = options.Host,
                                Port = options.Port,
                            },
                        ],
                    },
                ];
            }
        );

        services.AddSingleton<IQuartzJobStoreLockingManager, DistributedLocksQuartzLockingManager>();

        services.AddSingleton<IQuartzMongoDbJobStoreFactory, LocalQuartzMongoDbJobStoreFactory>();
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

                        storage.UseSystemTextJsonSerializer();

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
