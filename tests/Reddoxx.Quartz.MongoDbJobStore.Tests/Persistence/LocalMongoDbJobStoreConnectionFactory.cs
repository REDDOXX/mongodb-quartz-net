using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Database;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.Persistence;

internal class LocalMongoDbJobStoreConnectionFactory : IMongoDbJobStoreConnectionFactory
{
    private const string LocalConnectionString = "mongodb://localhost/quartz?minPoolSize=16&maxConnecting=32";

    private readonly IMongoDatabase _database;

    public LocalMongoDbJobStoreConnectionFactory()
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
