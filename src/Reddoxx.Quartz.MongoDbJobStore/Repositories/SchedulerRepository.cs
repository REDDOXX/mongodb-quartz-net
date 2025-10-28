using MongoDB.Driver;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class SchedulerRepository : BaseRepository<Scheduler>
{
    public SchedulerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "schedulers", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        // PRIMARY KEY (sched_name,instance_name)
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<Scheduler>(
                IndexBuilder.Combine(
                    //
                    IndexBuilder.Ascending(x => x.SchedulerName),
                    IndexBuilder.Ascending(x => x.InstanceId)
                ),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }

    public async Task AddScheduler(string instanceId, DateTimeOffset checkInTime, TimeSpan interval)
    {
        // INSERT INTO SCHEDULER_STATE (SCHED_NAME, INSTANCE_NAME, LAST_CHECKIN_TIME, CHECKIN_INTERVAL)
        // VALUES(@schedulerName, @instanceName, @lastCheckinTime, @checkinInterval)

        // AddCommandParameter(cmd, "schedulerName", schedName);
        // AddCommandParameter(cmd, "instanceName", instanceName);

        await Collection.InsertOneAsync(new Scheduler(InstanceName, instanceId, checkInTime, interval));
    }

    /// <summary>
    /// Delete a scheduler-instance state record.
    /// </summary>
    /// <param name="instanceId"></param>
    /// <returns></returns>
    public async Task DeleteScheduler(string instanceId)
    {
        // DELETE FROM SCHEDULER_STATE WHERE SCHED_NAME = @schedulerName AND INSTANCE_NAME = @instanceName

        var filter = FilterBuilder.Eq(x => x.SchedulerName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId);

        await Collection.DeleteOneAsync(filter);
    }

    public async Task<long> UpdateState(string instanceId, DateTimeOffset lastCheckIn)
    {
        // UPDATE SCHEDULER_STATE SET LAST_CHECKIN_TIME = @lastCheckinTime WHERE SCHED_NAME = @schedulerName AND INSTANCE_NAME = @instanceName

        // schedName = args.InstanceName;
        // AddCommandParameter(cmd, "schedulerName", args.InstanceName);
        //AddCommandParameter(cmd, "instanceName", instanceName);

        var filter = FilterBuilder.Eq(x => x.SchedulerName, InstanceName) &
                     FilterBuilder.Eq(x => x.InstanceId, instanceId);

        var update = UpdateBuilder.Set(sch => sch.LastCheckIn, lastCheckIn.UtcDateTime);

        var result = await Collection.UpdateOneAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<List<Scheduler>> SelectSchedulerStateRecords(string? instanceId)
    {
        // SELECT * FROM SCHEDULER_STATE WHERE SCHED_NAME = @schedulerName AND INSTANCE_NAME = @instanceName
        // SELECT * FROM SCHEDULER_STATE WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.SchedulerName, InstanceName);

        if (instanceId != null)
        {
            filter &= FilterBuilder.Eq(x => x.InstanceId, instanceId);
        }

        return await Collection.Find(filter)
                               .ToListAsync();
    }
}
