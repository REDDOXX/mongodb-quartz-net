using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Database;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.Persistence;

internal sealed class LocalQuartzMongoDbJobStoreFactory : IQuartzMongoDbJobStoreFactory
{
    private const string LocalConnectionString = "mongodb://localhost/quartz";

    private readonly IMongoDatabase _database;

    public LocalQuartzMongoDbJobStoreFactory()
    {
        var url = new MongoUrl(LocalConnectionString);
        var client = new MongoClient(url);

        _database = client.GetDatabase(url.DatabaseName);
    }

    public IMongoDatabase GetDatabase()
    {
        return _database;
    }
}
