using MongoDB.Driver;

using Quartz.Impl.Matchers;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

[CollectionName("pausedTriggerGroups")]
internal class PausedTriggerGroupRepository : BaseRepository<PausedTriggerGroup>
{
    public PausedTriggerGroupRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
        : base(database, instanceName, collectionPrefix)
    {
    }

    public async Task<List<string>> GetPausedTriggerGroups()
    {
        return await Collection.Find(group => group.Id.InstanceName == InstanceName)
            .Project(group => group.Id.Group)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<bool> IsTriggerGroupPaused(string group)
    {
        return await Collection.Find(g => g.Id == new PausedTriggerGroupId(group, InstanceName))
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task AddPausedTriggerGroup(string group)
    {
        await Collection.InsertOneAsync(
                new PausedTriggerGroup()
                {
                    Id = new PausedTriggerGroupId(group, InstanceName),
                }
            )
            .ConfigureAwait(false);
    }

    public async Task DeletePausedTriggerGroup(GroupMatcher<TriggerKey> matcher)
    {
        var regex = matcher.ToBsonRegularExpression().ToRegex();
        await Collection
            .DeleteManyAsync(group => group.Id.InstanceName == InstanceName && regex.IsMatch(group.Id.Group))
            .ConfigureAwait(false);
    }

    public async Task DeletePausedTriggerGroup(string groupName)
    {
        await Collection.DeleteOneAsync(group => group.Id == new PausedTriggerGroupId(groupName, InstanceName))
            .ConfigureAwait(false);
    }
}
