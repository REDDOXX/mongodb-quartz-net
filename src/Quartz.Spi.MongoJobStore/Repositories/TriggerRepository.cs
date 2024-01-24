using MongoDB.Driver;

using Quartz.Impl.Matchers;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal class TriggerRepository : BaseRepository<Trigger>
{
    public TriggerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "triggers", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<Trigger>(
                IndexBuilder.Combine(
                    IndexBuilder.Ascending(x => x.InstanceName),
                    IndexBuilder.Ascending(x => x.Name),
                    IndexBuilder.Ascending(x => x.Group)
                ),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }

    public async Task<bool> TriggerExists(TriggerKey key)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection
            //
            .Find(filter)
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task<bool> TriggersExists(string calendarName)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        return await Collection
            //
            .Find(filter)
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task<Trigger?> GetTrigger(TriggerKey key)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection
            //
            .Find(filter)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<Models.TriggerState> GetTriggerState(TriggerKey key)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection.Find(filter)
            .Project(trigger => trigger.State)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<JobDataMap> GetTriggerJobDataMap(TriggerKey key)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection.Find(filter)
            .Project(trigger => trigger.JobDataMap)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<Trigger>> GetTriggers(string calendarName)
    {
        var filter = FilterBuilder.Eq(x => x.CalendarName, calendarName);

        return await Collection
            //
            .Find(filter)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<Trigger>> GetTriggers(JobKey jobKey)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey, jobKey);

        return await Collection
            //
            .Find(filter)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression());

        return await Collection
            //
            .Find(filter)
            .Project(trigger => trigger.GetTriggerKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<TriggerKey>> GetTriggerKeys(Models.TriggerState state)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & // 
                     FilterBuilder.Eq(x => x.State, state);

        return await Collection.Find(filter)
            .Project(trigger => trigger.GetTriggerKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetTriggerGroupNames()
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
            //
            .Distinct(trigger => trigger.Group, filter)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetTriggerGroupNames(GroupMatcher<TriggerKey> matcher)
    {
        var regex = matcher.ToBsonRegularExpression().ToRegex();

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Regex(x => x.Group, regex);

        return await Collection
            //
            .Distinct(trigger => trigger.Group, filter)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<TriggerKey>> GetTriggersToAcquire(
        DateTimeOffset noLaterThan,
        DateTimeOffset noEarlierThan,
        int maxCount
    )
    {
        if (maxCount < 1)
        {
            maxCount = 1;
        }

        var noLaterThanDateTime = noLaterThan.UtcDateTime;
        var noEarlierThanDateTime = noEarlierThan.UtcDateTime;

        var filter = FilterBuilder.Where(
            x => x.InstanceName == InstanceName &&
                 x.State == Models.TriggerState.Waiting &&
                 x.NextFireTime <= noLaterThanDateTime &&
                 (x.MisfireInstruction == -1 || (x.MisfireInstruction != -1 && x.NextFireTime >= noEarlierThanDateTime))
        );

        var sort = SortBuilder.Combine(
            SortBuilder.Ascending(trigger => trigger.NextFireTime),
            SortBuilder.Descending(trigger => trigger.Priority)
        );

        return await Collection.Find(filter)
            .Sort(sort)
            .Limit(maxCount)
            .Project(trigger => trigger.GetTriggerKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetCount()
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
            //
            .Find(filter)
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetCount(JobKey jobKey)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey, jobKey);

        return await Collection
            //
            .Find(filter)
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetMisfireCount(DateTime nextFireTime)
    {
        var filter = FilterBuilder.Where(
            x => x.InstanceName == InstanceName &&
                 x.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy &&
                 x.NextFireTime < nextFireTime &&
                 x.State == Models.TriggerState.Waiting
        );

        return await Collection.Find(filter).CountDocumentsAsync().ConfigureAwait(false);
    }

    public async Task AddTrigger(Trigger trigger)
    {
        await Collection.InsertOneAsync(trigger).ConfigureAwait(false);
    }

    public async Task UpdateTrigger(Trigger trigger)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, trigger.InstanceName) &
                     FilterBuilder.Eq(x => x.Name, trigger.Name) &
                     FilterBuilder.Eq(x => x.Group, trigger.Group);

        await Collection.ReplaceOneAsync(filter, trigger).ConfigureAwait(false);
    }

    public async Task<long> UpdateTriggerState(TriggerKey triggerKey, Models.TriggerState state)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, triggerKey.Name) &
                     FilterBuilder.Eq(x => x.Group, triggerKey.Group);

        var update = UpdateBuilder.Set(trigger => trigger.State, state);

        var result = await Collection.UpdateOneAsync(filter, update).ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggerState(
        TriggerKey triggerKey,
        Models.TriggerState newState,
        Models.TriggerState oldState
    )
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, triggerKey.Name) &
                     FilterBuilder.Eq(x => x.Group, triggerKey.Group) &
                     FilterBuilder.Eq(x => x.State, oldState);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateOneAsync(filter, update).ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(
        GroupMatcher<TriggerKey> matcher,
        Models.TriggerState newState,
        params Models.TriggerState[] oldStates
    )
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression()) &
                     FilterBuilder.In(x => x.State, oldStates);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update).ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(
        JobKey jobKey,
        Models.TriggerState newState,
        params Models.TriggerState[] oldStates
    )
    {
        var filter = FilterBuilder.Where(
            x => x.InstanceName == InstanceName && x.JobKey == jobKey && oldStates.Contains(x.State)
        );

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update).ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(JobKey jobKey, Models.TriggerState newState)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey, jobKey);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update).ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(Models.TriggerState newState, params Models.TriggerState[] oldStates)
    {
        var filter = FilterBuilder.Where(x => x.InstanceName == InstanceName && oldStates.Contains(x.State));

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update).ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> DeleteTrigger(TriggerKey key)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        var result = await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
        return result.DeletedCount;
    }

    public async Task<long> DeleteTriggers(JobKey jobKey)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey, jobKey);

        var result = await Collection.DeleteManyAsync(filter).ConfigureAwait(false);
        return result.DeletedCount;
    }

    /// <summary>
    /// Get the names of all of the triggers in the given state that have
    /// misfired - according to the given timestamp.  No more than count will
    /// be returned.
    /// </summary>
    /// <param name="nextFireTime"></param>
    /// <param name="maxResults"></param>
    /// <param name="results"></param>
    /// <returns></returns>
    public bool HasMisfiredTriggers(DateTime nextFireTime, int maxResults, out List<TriggerKey> results)
    {
        results = [];

        var filter = FilterBuilder.Where(
            x => x.InstanceName == InstanceName &&
                 x.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy &&
                 x.NextFireTime < nextFireTime &&
                 x.State == Models.TriggerState.Waiting
        );

        var sort = SortBuilder.Combine(
            SortBuilder.Ascending(trigger => trigger.NextFireTime),
            SortBuilder.Descending(trigger => trigger.Priority)
        );

        // Perform query
        var cursor = Collection
            //
            .Find(filter)
            .Project(trigger => trigger.GetTriggerKey())
            .Sort(sort)
            .ToCursor();


        var hasReachedLimit = false;
        while (cursor.MoveNext() && !hasReachedLimit)
        {
            foreach (var triggerKey in cursor.Current)
            {
                if (results.Count == maxResults)
                {
                    hasReachedLimit = true;
                }
                else
                {
                    results.Add(triggerKey);
                }
            }
        }

        return hasReachedLimit;
    }
}
