using Microsoft.Extensions.Logging;

using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Util;
using Quartz.Util;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal interface IRepository
{
    Task EnsureIndex();
}

internal abstract class BaseRepository<TDocument> : IRepository
{
    private readonly ILogger _logger = LogProvider.CreateLogger<BaseRepository<TDocument>>();

    /// <summary>
    /// Also called schedName
    /// </summary>
    protected string InstanceName { get; }


    protected IMongoCollection<TDocument> Collection { get; }


    protected FilterDefinitionBuilder<TDocument> FilterBuilder => Builders<TDocument>.Filter;

    protected UpdateDefinitionBuilder<TDocument> UpdateBuilder => Builders<TDocument>.Update;

    protected SortDefinitionBuilder<TDocument> SortBuilder => Builders<TDocument>.Sort;

    protected ProjectionDefinitionBuilder<TDocument> ProjectionBuilder => Builders<TDocument>.Projection;

    protected IndexKeysDefinitionBuilder<TDocument> IndexBuilder => Builders<TDocument>.IndexKeys;


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


    protected string GetStorableJobTypeName(Type jobType)
    {
        if (jobType.AssemblyQualifiedName == null)
        {
            throw new ArgumentException("Cannot determine job type name when type's AssemblyQualifiedName is null");
        }

        return jobType.AssemblyQualifiedNameWithoutVersion();
    }
}
