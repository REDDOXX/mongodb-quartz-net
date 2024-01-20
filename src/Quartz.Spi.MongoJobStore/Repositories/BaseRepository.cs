using Common.Logging;

using MongoDB.Driver;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal abstract class BaseRepository<TDocument>
{
    private static readonly ILog Log = LogManager.GetLogger(typeof(BaseRepository<>));
    private static readonly HashSet<string> InitializedCollections = new();

    protected BaseRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
    {
        InstanceName = instanceName;
        var collectionName = GetCollectionName();
        if (!string.IsNullOrEmpty(collectionPrefix))
        {
            collectionName = $"{collectionPrefix}.{collectionName}";
        }

        Collection = database.GetCollection<TDocument>(collectionName);
        EnsureIndexesCreated(collectionName);
    }


    protected string InstanceName { get; }

    protected IMongoCollection<TDocument> Collection { get; }

    protected FilterDefinitionBuilder<TDocument> FilterBuilder => Builders<TDocument>.Filter;

    protected UpdateDefinitionBuilder<TDocument> UpdateBuilder => Builders<TDocument>.Update;

    protected SortDefinitionBuilder<TDocument> SortBuilder => Builders<TDocument>.Sort;

    protected ProjectionDefinitionBuilder<TDocument> ProjectionBuilder => Builders<TDocument>.Projection;

    protected IndexKeysDefinitionBuilder<TDocument> IndexBuilder => Builders<TDocument>.IndexKeys;

    public virtual Task EnsureIndex()
    {
        return Task.FromResult(0);
    }

    public async Task DeleteAll()
    {
        var filter = Builders<TDocument>.Filter.Empty;

        await Collection.DeleteManyAsync(filter).ConfigureAwait(false);
    }

    /// <summary>
    ///     Determines the collectionname
    /// </summary>
    /// <returns>Returns the collectionname.</returns>
    private string GetCollectionName()
    {
        // Check to see if the object (inherited from Entity) has a CollectionName attribute
        var att = Attribute.GetCustomAttribute(GetType(), typeof(CollectionName));
        var collectionname = att != null ? ((CollectionName)att).Name : typeof(TDocument).Name;

        return collectionname;
    }

    private void EnsureIndexesCreated(string collectionName)
    {
        if (InitializedCollections.Contains(collectionName))
        {
            return;
        }

        lock (InitializedCollections)
        {
            if (InitializedCollections.Contains(collectionName))
            {
                return;
            }

            Log.Trace($"Building index for {collectionName}");
            EnsureIndex();
            InitializedCollections.Add(collectionName);
        }
    }
}
