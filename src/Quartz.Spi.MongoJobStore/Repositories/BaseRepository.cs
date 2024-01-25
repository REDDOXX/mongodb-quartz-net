using MongoDB.Driver;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal interface IRepository
{
    Task EnsureIndex();
}

internal abstract class BaseRepository<TDocument> : IRepository
{
    /// <summary>
    /// Also called schedName
    /// </summary>
    protected string InstanceName { get; }


    public IMongoCollection<TDocument> Collection { get; }


    protected static FilterDefinitionBuilder<TDocument> FilterBuilder => Builders<TDocument>.Filter;

    protected static UpdateDefinitionBuilder<TDocument> UpdateBuilder => Builders<TDocument>.Update;

    protected static SortDefinitionBuilder<TDocument> SortBuilder => Builders<TDocument>.Sort;

    protected static ProjectionDefinitionBuilder<TDocument> ProjectionBuilder => Builders<TDocument>.Projection;

    protected static IndexKeysDefinitionBuilder<TDocument> IndexBuilder => Builders<TDocument>.IndexKeys;


    protected BaseRepository(
        IMongoDatabase database,
        string collectionName,
        string instanceName,
        string? collectionPrefix = null
    )
    {
        InstanceName = instanceName;

        if (!string.IsNullOrEmpty(collectionPrefix))
        {
            collectionName = $"{collectionPrefix}.{collectionName}";
        }

        Collection = database.GetCollection<TDocument>(collectionName);
    }


    public abstract Task EnsureIndex();


    public async Task DeleteAll()
    {
        var filter = Builders<TDocument>.Filter.Empty;

        await Collection.DeleteManyAsync(filter).ConfigureAwait(false);
    }
}
