using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

[CollectionName("schedulers")]
internal class SchedulerRepository : BaseRepository<Scheduler>
{
    public SchedulerRepository(IMongoDatabase database, string instanceName, string collectionPrefix = null)
        : base(database, instanceName, collectionPrefix)
    {
    }

    public async Task AddScheduler(Scheduler scheduler)
    {
        await Collection.ReplaceOneAsync(
                sch => sch.Id == scheduler.Id,
                scheduler,
                new UpdateOptions()
                {
                    IsUpsert = true,
                }
            )
            .ConfigureAwait(false);
    }

    public async Task DeleteScheduler(string id)
    {
        await Collection.DeleteOneAsync(sch => sch.Id == new SchedulerId(id, InstanceName)).ConfigureAwait(false);
    }

    public async Task UpdateState(string id, SchedulerState state)
    {
        await Collection.UpdateOneAsync(
                sch => sch.Id == new SchedulerId(id, InstanceName),
                UpdateBuilder.Set(sch => sch.State, state)
            )
            .ConfigureAwait(false);
    }
}
