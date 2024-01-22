using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Database;

namespace Quartz.Spi.MongoJobStore.Tests.DependencyInjection;

internal class LocalMongoDbJobStoreConnectionFactory : IMongoDbJobStoreConnectionFactory
{
    private const string LocalConnectionString = "mongodb://localhost/quartz";

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
