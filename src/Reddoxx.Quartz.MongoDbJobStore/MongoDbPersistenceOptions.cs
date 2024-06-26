using JetBrains.Annotations;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore;

[PublicAPI]
public class MongoDbPersistenceOptions
{
    private readonly SchedulerBuilder.PersistentStoreOptions _options;

    internal MongoDbPersistenceOptions(SchedulerBuilder.PersistentStoreOptions options)
    {
        _options = options;
    }

    public string CollectionPrefix
    {
        set => _options.SetProperty("quartz.jobStore.collectionPrefix", value);
    }
}
