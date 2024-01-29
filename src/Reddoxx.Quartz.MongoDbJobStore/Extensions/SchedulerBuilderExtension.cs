using JetBrains.Annotations;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore.Extensions;

[PublicAPI]
public static class SchedulerBuilderExtension
{
    public static void ConfigureMongoDb(
        this SchedulerBuilder.PersistentStoreOptions options,
        Action<MongoDbPersistenceOptions> configure
    )
    {
        configure.Invoke(new MongoDbPersistenceOptions(options));
    }
}
