using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal class SchedulerRepository : BaseRepository<Scheduler>
{
    public SchedulerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "schedulers", instanceName, collectionPrefix)
    {
    }

    public override Task EnsureIndex()
    {
        return Task.CompletedTask;
    }

    public async Task AddScheduler(Scheduler scheduler)
    {
        await Collection.ReplaceOneAsync(
                sch => sch.Id == scheduler.Id,
                scheduler,
                new ReplaceOptions
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
        var update = UpdateBuilder.Set(sch => sch.State, state);

        await Collection.UpdateOneAsync(sch => sch.Id == new SchedulerId(id, InstanceName), update)
            .ConfigureAwait(false);
    }
}
