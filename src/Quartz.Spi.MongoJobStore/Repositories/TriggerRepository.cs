using MongoDB.Driver;

using Quartz.Impl.Matchers;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

[CollectionName("triggers")]
internal class TriggerRepository : BaseRepository<Trigger>
{
    public TriggerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, instanceName, collectionPrefix)
    {
    }

    public async Task<bool> TriggerExists(TriggerKey key)
    {
        return await Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName))
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task<bool> TriggersExists(string calendarName)
    {
        return await Collection
            .Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.CalendarName == calendarName)
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task<Trigger?> GetTrigger(TriggerKey key)
    {
        return await Collection.Find(trigger => trigger.Id == new TriggerId(key, InstanceName))
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<Models.TriggerState> GetTriggerState(TriggerKey triggerKey)
    {
        return await Collection.Find(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName))
            .Project(trigger => trigger.State)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<JobDataMap> GetTriggerJobDataMap(TriggerKey triggerKey)
    {
        return await Collection.Find(trigger => trigger.Id == new TriggerId(triggerKey, InstanceName))
            .Project(trigger => trigger.JobDataMap)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<Trigger>> GetTriggers(string calendarName)
    {
        return await Collection.Find(FilterBuilder.Where(trigger => trigger.CalendarName == calendarName))
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<Trigger>> GetTriggers(JobKey jobKey)
    {
        return await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
    {
        var filter = FilterBuilder.And(
            FilterBuilder.Eq(trigger => trigger.Id.InstanceName, InstanceName),
            FilterBuilder.Regex(trigger => trigger.Id.Group, matcher.ToBsonRegularExpression())
        );

        return await Collection
            //
            .Find(filter)
            .Project(trigger => trigger.Id.GetTriggerKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<TriggerKey>> GetTriggerKeys(Models.TriggerState state)
    {
        return await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName && trigger.State == state)
            .Project(trigger => trigger.Id.GetTriggerKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetTriggerGroupNames()
    {
        return await Collection
            .Distinct(trigger => trigger.Id.Group, trigger => trigger.Id.InstanceName == InstanceName)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetTriggerGroupNames(GroupMatcher<TriggerKey> matcher)
    {
        var regex = matcher.ToBsonRegularExpression().ToRegex();
        return await Collection.Distinct(
                trigger => trigger.Id.Group,
                trigger => trigger.Id.InstanceName == InstanceName && regex.IsMatch(trigger.Id.Group)
            )
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

        return await Collection
            .Find(
                trigger => trigger.Id.InstanceName == InstanceName &&
                           trigger.State == Models.TriggerState.Waiting &&
                           trigger.NextFireTime <= noLaterThanDateTime &&
                           (trigger.MisfireInstruction == -1 ||
                            (trigger.MisfireInstruction != -1 && trigger.NextFireTime >= noEarlierThanDateTime))
            )
            .Sort(
                SortBuilder.Combine(
                    SortBuilder.Ascending(trigger => trigger.NextFireTime),
                    SortBuilder.Descending(trigger => trigger.Priority)
                )
            )
            .Limit(maxCount)
            .Project(trigger => trigger.Id.GetTriggerKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetCount()
    {
        return await Collection.Find(trigger => trigger.Id.InstanceName == InstanceName)
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetCount(JobKey jobKey)
    {
        return await Collection
            .Find(FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey))
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetMisfireCount(DateTime nextFireTime)
    {
        return await Collection.Find(
                trigger => trigger.Id.InstanceName == InstanceName &&
                           trigger.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy &&
                           trigger.NextFireTime < nextFireTime &&
                           trigger.State == Models.TriggerState.Waiting
            )
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }

    public async Task AddTrigger(Trigger trigger)
    {
        await Collection.InsertOneAsync(trigger).ConfigureAwait(false);
    }

    public async Task UpdateTrigger(Trigger trigger)
    {
        await Collection.ReplaceOneAsync(t => t.Id == trigger.Id, trigger).ConfigureAwait(false);
    }

    public async Task<long> UpdateTriggerState(TriggerKey triggerKey, Models.TriggerState state)
    {
        var update = UpdateBuilder.Set(trigger => trigger.State, state);

        var result = await Collection.UpdateOneAsync(
                trigger => trigger.Id == new TriggerId(triggerKey, InstanceName),
                update
            )
            .ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggerState(
        TriggerKey triggerKey,
        Models.TriggerState newState,
        Models.TriggerState oldState
    )
    {
        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateOneAsync(
                trigger => trigger.Id == new TriggerId(triggerKey, InstanceName) && trigger.State == oldState,
                update
            )
            .ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(
        GroupMatcher<TriggerKey> matcher,
        Models.TriggerState newState,
        params Models.TriggerState[] oldStates
    )
    {
        var filter = FilterBuilder.And(
            FilterBuilder.Eq(trigger => trigger.Id.InstanceName, InstanceName),
            FilterBuilder.Regex(trigger => trigger.Id.Group, matcher.ToBsonRegularExpression()),
            FilterBuilder.In(trigger => trigger.State, oldStates)
        );

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
        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(
                trigger => trigger.Id.InstanceName == InstanceName &&
                           trigger.JobKey == jobKey &&
                           oldStates.Contains(trigger.State),
                update
            )
            .ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(JobKey jobKey, Models.TriggerState newState)
    {
        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(
                trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey,
                update
            )
            .ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(Models.TriggerState newState, params Models.TriggerState[] oldStates)
    {
        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(
                trigger => trigger.Id.InstanceName == InstanceName && oldStates.Contains(trigger.State),
                update
            )
            .ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task<long> DeleteTrigger(TriggerKey key)
    {
        var filter = FilterBuilder.Where(trigger => trigger.Id == new TriggerId(key, InstanceName));

        var result = await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
        return result.DeletedCount;
    }

    public async Task<long> DeleteTriggers(JobKey jobKey)
    {
        var filter =
            FilterBuilder.Where(trigger => trigger.Id.InstanceName == InstanceName && trigger.JobKey == jobKey);

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
        var cursor = Collection.Find(
                trigger => trigger.Id.InstanceName == InstanceName &&
                           trigger.MisfireInstruction != MisfireInstruction.IgnoreMisfirePolicy &&
                           trigger.NextFireTime < nextFireTime &&
                           trigger.State == Models.TriggerState.Waiting
            )
            .Project(trigger => trigger.Id.GetTriggerKey())
            .Sort(
                SortBuilder.Combine(
                    SortBuilder.Ascending(trigger => trigger.NextFireTime),
                    SortBuilder.Descending(trigger => trigger.Priority)
                )
            )
            .ToCursor();

        results = new List<TriggerKey>();

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
