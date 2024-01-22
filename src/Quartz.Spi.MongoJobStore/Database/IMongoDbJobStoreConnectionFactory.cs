using JetBrains.Annotations;

using MongoDB.Driver;

namespace Quartz.Spi.MongoJobStore.Database;

[PublicAPI]
public interface IMongoDbJobStoreConnectionFactory
{
    /// <summary>
    /// Returns the database where the quartz collections reside in
    /// </summary>
    /// <returns></returns>
    IMongoDatabase GetDatabase();
}
