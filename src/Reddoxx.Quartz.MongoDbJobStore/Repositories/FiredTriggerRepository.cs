using MongoDB.Driver;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class FiredTriggerRepository : BaseRepository<FiredTrigger>
{
    public FiredTriggerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "firedTriggers", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        var indices = new List<CreateIndexModel<FiredTrigger>>
        {
            // PK_QRTZ_FIRED_TRIGGERS
            new(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.FiredInstanceId),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            ),

            // CREATE INDEX [IDX_QRTZ_FT_INST_JOB_REQ_RCVRY] ON [dbo].[QRTZ_FIRED_TRIGGERS](SCHED_NAME, INSTANCE_NAME, REQUESTS_RECOVERY);
            new(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.InstanceId)
                            .Ascending(x => x.RequestsRecovery),
                new CreateIndexOptions<FiredTrigger>
                {
                    PartialFilterExpression = FilterBuilder.Eq(x => x.RequestsRecovery, true),
                }
            ),

            // CREATE INDEX [IDX_QRTZ_FT_G_J] ON [dbo].[QRTZ_FIRED_TRIGGERS](SCHED_NAME, JOB_GROUP, JOB_NAME);
            new(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.JobKey.Group)
                            .Ascending(x => x.JobKey.Name)
            ),

            // CREATE INDEX [IDX_QRTZ_FT_G_T] ON [dbo].[QRTZ_FIRED_TRIGGERS](SCHED_NAME, TRIGGER_GROUP, TRIGGER_NAME);
            new(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.TriggerKey.Group)
                            .Ascending(x => x.TriggerKey.Name)
            ),
        };

        await Collection.Indexes.CreateManyAsync(indices);
    }


    public async Task<List<FiredTrigger>> GetFiredTriggers(JobKey jobKey)
    {
        // SELECT *
        // FROM FIRED_TRIGGERS
        // WHERE
        //   SCHED_NAME = @schedulerName AND
        //   JOB_NAME = @jobName AND
        //   JOB_GROUP = @jobGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.JobKey.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.JobKey.Group, jobKey.Group);

        return await Collection
                     //
                     .Find(filter)
                     .ToListAsync();
    }

    public async Task<List<FiredTrigger>> SelectInstancesFiredTriggerRecords(string instanceId)
    {
        // SELECT * FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName AND INSTANCE_NAME = @instanceName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId);

        return await Collection.Find(filter)
                               .ToListAsync();
    }

    /// <summary>
    /// Select the distinct instance names of all fired-trigger records.
    /// </summary>
    /// <returns></returns>
    public async Task<List<string>> SelectFiredTriggerInstanceIds()
    {
        // SELECT DISTINCT INSTANCE_NAME FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection.Distinct(x => x.InstanceId, filter)
                               .ToListAsync();
    }

    public async Task<List<FiredTrigger>> SelectFiredTriggerRecords(string? triggerName, string group)
    {
        // SELECT * FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup
        // SELECT * FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP = @triggerGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.TriggerKey.Group, group);

        if (triggerName != null)
        {
            filter &= FilterBuilder.Eq(x => x.TriggerKey.Name, triggerName);
        }

        return await Collection.Find(filter)
                               .ToListAsync();
    }

    public async Task<List<FiredTrigger>> GetRecoverableFiredTriggers(string instanceId)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId) &
                     FilterBuilder.Eq(x => x.RequestsRecovery, true);

        return await Collection.Find(filter)
                               .ToListAsync();
    }

    public async Task AddFiredTrigger(FiredTrigger firedTrigger)
    {
        await Collection.InsertOneAsync(firedTrigger);
    }

    public async Task DeleteFiredTrigger(string firedInstanceId)
    {
        // DELETE FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName AND ENTRY_ID = @triggerEntryId

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.FiredInstanceId, firedInstanceId);

        await Collection.DeleteOneAsync(filter);
    }

    /// <summary>
    /// Delete all fired triggers of the given instance.
    /// </summary>
    /// <param name="instanceId"></param>
    /// <returns></returns>
    public async Task<long> DeleteFiredTriggersByInstanceId(string instanceId)
    {
        // DELETE FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName AND INSTANCE_NAME = @instanceName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId);

        var result = await Collection.DeleteManyAsync(filter);
        return result.DeletedCount;
    }

    /// <summary>
    /// Delete all fired triggers.
    /// </summary>
    /// <returns></returns>
    public async Task<long> DeleteFiredTriggers()
    {
        // DELETE FROM FIRED_TRIGGERS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        var result = await Collection.DeleteManyAsync(filter);
        return result.DeletedCount;
    }


    public async Task UpdateFiredTrigger(FiredTrigger firedTrigger)
    {
        // UPDATE FIRED_TRIGGERS
        // SET
        //  INSTANCE_NAME = @instanceName,
        //  FIRED_TIME = @firedTime,
        //  SCHED_TIME = @scheduledTime,
        //  STATE = @entryState,
        //  JOB_NAME = @jobName,
        //  JOB_GROUP = @jobGroup,
        //  IS_NONCONCURRENT = @isNonConcurrent,
        //  REQUESTS_RECOVERY = @requestsRecover
        // WHERE
        //  SCHED_NAME = @schedulerName AND ENTRY_ID = @entryId";


        var filter = FilterBuilder.Eq(x => x.InstanceName, firedTrigger.InstanceName) &
                     FilterBuilder.Eq(x => x.FiredInstanceId, firedTrigger.FiredInstanceId);

        var update = UpdateBuilder
                     //
                     .Set(x => x.InstanceId, firedTrigger.InstanceId)
                     .Set(x => x.Fired, firedTrigger.Fired)
                     .Set(x => x.Scheduled, firedTrigger.Scheduled)
                     .Set(x => x.State, firedTrigger.State)
                     .Set(x => x.JobKey, firedTrigger.JobKey)
                     .Set(x => x.ConcurrentExecutionDisallowed, firedTrigger.ConcurrentExecutionDisallowed)
                     .Set(x => x.RequestsRecovery, firedTrigger.RequestsRecovery);

        await Collection.UpdateOneAsync(filter, update);
    }
}
