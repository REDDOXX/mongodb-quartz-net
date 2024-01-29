using MongoDB.Driver;

using Quartz;
using Quartz.Impl.Matchers;

using Reddoxx.Quartz.MongoDbJobStore.Extensions;
using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class PausedTriggerGroupRepository : BaseRepository<PausedTriggerGroup>
{
    public PausedTriggerGroupRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "pausedTriggerGroups", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        // PRIMARY KEY (sched_name,trigger_group)
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<PausedTriggerGroup>(
                IndexBuilder.Combine(
                    //
                    IndexBuilder.Ascending(x => x.InstanceName),
                    IndexBuilder.Ascending(x => x.Group)
                ),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }


    /// <summary>
    /// Selects the paused trigger groups.
    /// </summary>
    /// <returns></returns>
    public async Task<List<string>> GetPausedTriggerGroups()
    {
        // SELECT TRIGGER_GROUP FROM PAUSED_TRIGGER_GRPS WHERE SCHED_NAME = @schedulerName

        return await Collection
            //
            .Find(group => group.InstanceName == InstanceName)
            .Project(group => group.Group)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<bool> IsTriggerGroupPaused(string group)
    {
        // SELECT TRIGGER_GROUP FROM PAUSED_TRIGGER_GRPS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP = @triggerGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.Group, group);

        return await Collection
            //
            .Find(filter)
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task AddPausedTriggerGroup(string group)
    {
        // INSERT INTO PAUSED_TRIGGER_GRPS (SCHED_NAME, TRIGGER_GROUP) VALUES (@schedulerName, @triggerGroup)
        await Collection.InsertOneAsync(
                new PausedTriggerGroup
                {
                    InstanceName = InstanceName,
                    Group = group,
                }
            )
            .ConfigureAwait(false);
    }

    public async Task DeletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher)
    {
        // DELETE FROM PAUSED_TRIGGER_GRPS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP LIKE @triggerGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression());

        await Collection.DeleteManyAsync(filter).ConfigureAwait(false);
    }

    public async Task DeletePausedTriggerGroup(string groupName)
    {
        // DELETE FROM PAUSED_TRIGGER_GRPS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP LIKE @triggerGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & // 
                     FilterBuilder.Eq(x => x.Group, groupName);

        await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
    }
}
