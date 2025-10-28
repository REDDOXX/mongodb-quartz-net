using Microsoft.Extensions.Options;

using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;
using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Database;
using Reddoxx.Quartz.MongoDbJobStore.Tests.Options;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.Persistence;

internal sealed class LocalQuartzMongoDbJobStoreFactory : IQuartzMongoDbJobStoreFactory
{
    private readonly IMongoDatabase _database;

    public LocalQuartzMongoDbJobStoreFactory(IOptions<MongoDbOptions> options)
    {
        // Set the default guid representation
        BsonSerializer.TryRegisterSerializer(new GuidSerializer(GuidRepresentation.Standard));

        var objectDiscriminatorConvention = BsonSerializer.LookupDiscriminatorConvention(typeof(object));
        var objectSerializer = new ObjectSerializer(objectDiscriminatorConvention, GuidRepresentation.Standard);
        BsonSerializer.TryRegisterSerializer(objectSerializer);

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
