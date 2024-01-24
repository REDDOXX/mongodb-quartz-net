using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal class FiredTriggerRepository : BaseRepository<FiredTrigger>
{
    public FiredTriggerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "firedTriggers", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(
                IndexBuilder.Combine(
                    IndexBuilder.Ascending(x => x.InstanceName),
                    IndexBuilder.Ascending(x => x.FiredInstanceId)
                ),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }

    public async Task<List<FiredTrigger>> GetFiredTriggers(JobKey jobKey)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey, jobKey);

        return await Collection
            //
            .Find(filter)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<FiredTrigger>> GetFiredTriggers(string instanceId)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId);

        return await Collection.Find(filter).ToListAsync().ConfigureAwait(false);
    }

    public async Task<List<FiredTrigger>> GetRecoverableFiredTriggers(string instanceId)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId) &
                     FilterBuilder.Eq(x => x.RequestsRecovery, true);

        return await Collection.Find(filter).ToListAsync().ConfigureAwait(false);
    }

    public async Task AddFiredTrigger(FiredTrigger firedTrigger)
    {
        await Collection.InsertOneAsync(firedTrigger).ConfigureAwait(false);
    }

    public async Task DeleteFiredTrigger(string firedInstanceId)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.FiredInstanceId, firedInstanceId);

        await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
    }

    public async Task<long> DeleteFiredTriggersByInstanceId(string instanceId)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId);

        var result = await Collection.DeleteManyAsync(filter).ConfigureAwait(false);
        return result.DeletedCount;
    }

    public async Task UpdateFiredTrigger(FiredTrigger firedTrigger)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, firedTrigger.InstanceName) &
                     FilterBuilder.Eq(x => x.FiredInstanceId, firedTrigger.FiredInstanceId);

        await Collection.ReplaceOneAsync(filter, firedTrigger).ConfigureAwait(false);
    }
}
