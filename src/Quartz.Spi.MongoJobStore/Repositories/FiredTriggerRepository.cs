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
        // PRIMARY KEY (sched_name,entry_id)
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

        // create index idx_qrtz_ft_trig_name on qrtz_fired_triggers(trigger_name);
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(IndexBuilder.Ascending(x => x.TriggerKey.Name))
        );

        // create index idx_qrtz_ft_trig_group on qrtz_fired_triggers(trigger_group);
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(IndexBuilder.Ascending(x => x.TriggerKey.Group))
        );

        // create index idx_qrtz_ft_trig_nm_gp on qrtz_fired_triggers(sched_name,trigger_name,trigger_group);
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(
                IndexBuilder.Combine(
                    IndexBuilder.Ascending(x => x.InstanceName),
                    IndexBuilder.Ascending(x => x.TriggerKey.Name),
                    IndexBuilder.Ascending(x => x.TriggerKey.Group)
                )
            )
        );

        // create index idx_qrtz_ft_trig_inst_name on qrtz_fired_triggers(instance_name);
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(IndexBuilder.Ascending(x => x.InstanceId))
        );

        // create index idx_qrtz_ft_job_name on qrtz_fired_triggers(job_name);
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(IndexBuilder.Ascending(x => x.JobKey.Name))
        );

        // create index idx_qrtz_ft_job_group on qrtz_fired_triggers(job_group);
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<FiredTrigger>(IndexBuilder.Ascending(x => x.JobKey.Group))
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

    /// <summary>
    /// Select the distinct instance names of all fired-trigger records.
    /// </summary>
    /// <returns></returns>
    public async Task<List<string>> SelectFiredTriggerInstanceNames()
    {
        // SELECT DISTINCT INSTANCE_NAME FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
            //
            .Distinct(x => x.InstanceId, filter)
            .ToListAsync()
            .ConfigureAwait(false);
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
