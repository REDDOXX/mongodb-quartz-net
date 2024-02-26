using Microsoft.Extensions.Options;

using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Database;
using Reddoxx.Quartz.MongoDbJobStore.Tests.Options;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.Persistence;

internal sealed class LocalQuartzMongoDbJobStoreFactory : IQuartzMongoDbJobStoreFactory
{
    private readonly IMongoDatabase _database;

    public LocalQuartzMongoDbJobStoreFactory(IOptions<MongoDbOptions> options)
    {
        var mongoDbOptions = options.Value;

        var url = new MongoUrl(mongoDbOptions.ConnectionString);
        var client = new MongoClient(url);

        _database = client.GetDatabase(url.DatabaseName);
    }

    public IMongoDatabase GetDatabase()
    {
        return _database;
    }
}
