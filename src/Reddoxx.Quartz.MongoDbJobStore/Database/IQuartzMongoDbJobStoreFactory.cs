using JetBrains.Annotations;

using MongoDB.Driver;

namespace Reddoxx.Quartz.MongoDbJobStore.Database;

[PublicAPI]
public interface IQuartzMongoDbJobStoreFactory
{
    /// <summary>
    /// Returns the database where the quartz collections reside in
    /// </summary>
    /// <returns></returns>
    IMongoDatabase GetDatabase();
}
