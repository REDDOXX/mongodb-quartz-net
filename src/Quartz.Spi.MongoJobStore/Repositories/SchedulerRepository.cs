using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal class SchedulerRepository : BaseRepository<Scheduler>
{
    public SchedulerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "schedulers", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<Scheduler>(
                IndexBuilder.Combine(
                    //
                    IndexBuilder.Ascending(x => x.InstanceName),
                    IndexBuilder.Ascending(x => x.Name)
                ),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }

    public async Task AddScheduler(Scheduler scheduler)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, scheduler.InstanceName) &
                     FilterBuilder.Eq(x => x.Name, scheduler.Name);

        await Collection.ReplaceOneAsync(
                filter,
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
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.Name, id);

        await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
    }

    public async Task UpdateState(string id, DateTime lastCheckIn)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.Name, id);

        var update = UpdateBuilder.Set(sch => sch.LastCheckIn, lastCheckIn);

        await Collection.UpdateOneAsync(filter, update).ConfigureAwait(false);
    }
}
