using JetBrains.Annotations;

namespace Quartz.Spi.MongoJobStore.Extensions;

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
