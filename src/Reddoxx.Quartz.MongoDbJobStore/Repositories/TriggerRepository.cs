using MongoDB.Driver;

using Quartz;
using Quartz.Impl.Matchers;

using Reddoxx.Quartz.MongoDbJobStore.Extensions;
using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class TriggerRepository : BaseRepository<Trigger>
{
    public TriggerRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "triggers", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        var indices = new List<CreateIndexModel<Trigger>>
        {
            new(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.Name)
                            .Ascending(x => x.Group),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            ),

            // create index idx_qrtz_t_next_fire_time on qrtz_triggers(next_fire_time);
            new(IndexBuilder.Ascending(x => x.NextFireTime)),

            // create index idx_qrtz_t_state on qrtz_triggers(trigger_state);
            new(IndexBuilder.Ascending(x => x.State)),

            // create index idx_qrtz_t_nft_st on qrtz_triggers(next_fire_time,trigger_state);
            new(
                IndexBuilder.Ascending(x => x.NextFireTime)
                            .Ascending(x => x.State)
            ),
        };

        await Collection.Indexes.CreateManyAsync(indices);
    }

    public async Task<bool> TriggerExists(TriggerKey key)
    {
        // SELECT 1 FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection.Find(filter)
                               .AnyAsync();
    }

    public async Task<bool> CalendarIsReferenced(string calendarName)
    {
        // SELECT 1 FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        return await Collection.Find(filter)
                               .AnyAsync();
    }

    public async Task<Trigger?> GetTrigger(TriggerKey key)
    {
        /*
            SELECT
                JOB_NAME,
                JOB_GROUP,
                DESCRIPTION,
                NEXT_FIRE_TIME,
                PREV_FIRE_TIME,
                TRIGGER_TYPE,
                START_TIME,
                END_TIME,
                CALENDAR_NAME,
                MISFIRE_INSTR,
                PRIORITY,
                JOB_DATA,
                CRON_EXPRESSION,
                TIME_ZONE_ID,
                REPEAT_COUNT,
                REPEAT_INTERVAL,
                TIMES_TRIGGERED
            FROM
                TRIGGERS t
            LEFT JOIN
                SIMPLE_TRIGGERS st ON (st.SCHED_NAME = t.SCHED_NAME AND st.TRIGGER_GROUP = t.TRIGGER_GROUP AND st.TRIGGER_NAME = t.TRIGGER_NAME)
            LEFT JOIN
                CRON_TRIGGERS ct ON (ct.SCHED_NAME = t.SCHED_NAME AND ct.TRIGGER_GROUP = t.TRIGGER_GROUP AND ct.TRIGGER_NAME = t.TRIGGER_NAME)
            WHERE
                t.SCHED_NAME = @schedulerName AND t.TRIGGER_NAME = @triggerName AND t.TRIGGER_GROUP = @triggerGroup";

        */

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection.Find(filter)
                               .FirstOrDefaultAsync();
    }

    public async Task<LocalTriggerState> GetTriggerState(TriggerKey key)
    {
        // SELECT TRIGGER_STATE
        // FROM TRIGGERS
        // WHERE
        //   SCHED_NAME = @schedulerName AND
        //   TRIGGER_NAME = @triggerName AND
        //   TRIGGER_GROUP = @triggerGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        var result = await Collection.Find(filter)
                                     .FirstOrDefaultAsync();
        if (result == null)
        {
            // We cannot use a simple projection here, as the TriggerState is a value object
            // which would construct it with = None as value.
            return LocalTriggerState.Deleted;
        }

        return result.State;
    }

    public async Task<JobDataMap?> GetTriggerJobDataMap(TriggerKey key)
    {
        // SELECT JOB_DATA FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        return await Collection.Find(filter)
                               .Project(trigger => trigger.JobDataMap)
                               .FirstOrDefaultAsync();
    }

    public async Task<List<Trigger>> SelectTriggersForCalendar(string calendarName)
    {
        // SELECT TRIGGER_NAME, TRIGGER_GROUP FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        return await Collection.Find(filter)
                               .ToListAsync();
    }

    public async Task<List<Trigger>> GetTriggers(JobKey jobKey)
    {
        // SELECT TRIGGER_NAME, TRIGGER_GROUP
        // FROM TRIGGERS
        // WHERE SCHED_NAME = @schedulerName AND JOB_NAME = @jobName AND JOB_GROUP = @jobGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.JobKey.Group, jobKey.Group);

        return await Collection.Find(filter)
                               .ToListAsync();
    }

    public async Task<List<TriggerKey>> GetTriggerKeys(GroupMatcher<TriggerKey> matcher)
    {
        // SELECT TRIGGER_NAME, TRIGGER_GROUP FROM {0}TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP = @triggerGroup
        // SELECT TRIGGER_NAME, TRIGGER_GROUP FROM {0}TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP LIKE @triggerGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression());

        return await Collection.Find(filter)
                               .Project(trigger => new TriggerKey(trigger.Name, trigger.Group))
                               .ToListAsync();
    }

    public async Task<List<TriggerKey>> GetTriggerKeys(LocalTriggerState state)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & // 
                     FilterBuilder.Eq(x => x.State, state);

        return await Collection.Find(filter)
                               .Project(trigger => new TriggerKey(trigger.Name, trigger.Group))
                               .ToListAsync();
    }

    public async Task<List<string>> GetTriggerGroupNames()
    {
        // SELECT DISTINCT(TRIGGER_GROUP) FROM TRIGGERS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
                     //
                     .Distinct(trigger => trigger.Group, filter)
                     .ToListAsync();
    }

    public async Task<List<string>> GetTriggerGroupNames(GroupMatcher<TriggerKey> matcher)
    {
        // SELECT DISTINCT(TRIGGER_GROUP) FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_GROUP LIKE @triggerGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression());

        return await Collection
                     //
                     .Distinct(trigger => trigger.Group, filter)
                     .ToListAsync();
    }

    /// <summary>
    /// Select the next trigger which will fire to fire between the two given timestamps 
    /// in ascending order of fire time, and then descending by priority.
    /// </summary>
    /// <param name="noLaterThan"></param>
    /// <param name="noEarlierThan"></param>
    /// <param name="maxCount"></param>
    /// <returns></returns>
    public async Task<List<TriggerKey>> GetTriggersToAcquire(
        DateTimeOffset noLaterThan,
        DateTimeOffset noEarlierThan,
        int maxCount
    )
    {
        if (maxCount < 1)
        {
            // we want at least one trigger back.
            maxCount = 1;
        }

        //  SELECT
        //      t.TRIGGER_NAME, t.TRIGGER_GROUP, jd.JOB_CLASS_NAME
        //  FROM
        //      TRIGGERS t
        //  JOIN
        //      JOB_DETAILS jd ON (jd.SCHED_NAME = t.SCHED_NAME AND jd.JOB_GROUP = t.JOB_GROUP AND jd.JOB_NAME = t.JOB_NAME)
        //  WHERE
        //      t.SCHED_NAME = @schedulerName AND
        //      TRIGGER_STATE = @state AND
        //      NEXT_FIRE_TIME <= @noLaterThan AND
        //      (MISFIRE_INSTR = -1 OR (MISFIRE_INSTR <> -1 AND NEXT_FIRE_TIME >= @noEarlierThan))
        //  ORDER BY
        //      NEXT_FIRE_TIME ASC, PRIORITY DESC

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.State, LocalTriggerState.Waiting) &
                     FilterBuilder.Lte(x => x.NextFireTime, noLaterThan) &
                     FilterBuilder.Or(
                         FilterBuilder.Eq(x => x.MisfireInstruction, MisfireInstruction.IgnoreMisfirePolicy),
                         FilterBuilder.And(
                             FilterBuilder.Ne(x => x.MisfireInstruction, MisfireInstruction.IgnoreMisfirePolicy),
                             FilterBuilder.Gte(x => x.NextFireTime, noEarlierThan)
                         )
                     );

        var sort = SortBuilder.Ascending(trigger => trigger.NextFireTime)
                              .Descending(trigger => trigger.Priority);

        return await Collection.Find(filter)
                               .Sort(sort)
                               .Limit(maxCount)
                               .Project(trigger => new TriggerKey(trigger.Name, trigger.Group))
                               .ToListAsync();
    }

    public async Task<long> GetCount()
    {
        // SELECT COUNT(TRIGGER_NAME)  FROM TRIGGERS WHERE SCHED_NAME = @schedulerName
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
                     //
                     .Find(filter)
                     .CountDocumentsAsync();
    }

    public async Task<long> GetCount(JobKey jobKey)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.JobKey.Group, jobKey.Group);

        return await Collection
                     //
                     .Find(filter)
                     .CountDocumentsAsync();
    }

    public async Task<long> GetMisfireCount(DateTimeOffset nextFireTime)
    {
        // SELECT
        //  COUNT(TRIGGER_NAME)
        // FROM TRIGGERS
        // WHERE
        //   SCHED_NAME = @schedulerName AND
        //   MISFIRE_INSTR <> {MisfireInstruction.IgnoreMisfirePolicy} AND
        //   NEXT_FIRE_TIME < @nextFireTime AND
        //   TRIGGER_STATE = @state1

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Ne(x => x.MisfireInstruction, MisfireInstruction.IgnoreMisfirePolicy) &
                     FilterBuilder.Lt(x => x.NextFireTime, nextFireTime) &
                     FilterBuilder.Eq(x => x.State, LocalTriggerState.Waiting);

        return await Collection.Find(filter)
                               .CountDocumentsAsync();
    }

    public async Task AddTrigger(Trigger trigger)
    {
        await Collection.InsertOneAsync(trigger);
    }

    public async Task UpdateTrigger(Trigger trigger)
    {
        // UPDATE TRIGGERS
        // SET
        //   JOB_NAME = @triggerJobName, 
        //   JOB_GROUP = @triggerJobGroup, 
        //   DESCRIPTION = @triggerDescription, 
        //   NEXT_FIRE_TIME = @triggerNextFireTime, 
        //   PREV_FIRE_TIME = @triggerPreviousFireTime,
        //   TRIGGER_STATE = @triggerState, 
        //   TRIGGER_TYPE = @triggerType, 
        //   START_TIME = @triggerStartTime, 
        //   END_TIME = @triggerEndTime, 
        //   CALENDAR_NAME = @triggerCalendarName, 
        //   MISFIRE_INSTR = @triggerMisfireInstruction, 
        //   PRIORITY = @triggerPriority,
        //   JOB_DATA = @triggerJobJobDataMap
        // WHERE
        //  SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup";

        var filter = FilterBuilder.Eq(x => x.InstanceName, trigger.InstanceName) &
                     FilterBuilder.Eq(x => x.Name, trigger.Name) &
                     FilterBuilder.Eq(x => x.Group, trigger.Group);

        var update = UpdateBuilder
                     // 
                     .Set(x => x.JobKey, trigger.JobKey)
                     .Set(x => x.Description, trigger.Description)
                     .Set(x => x.NextFireTime, trigger.NextFireTime)
                     .Set(x => x.PreviousFireTime, trigger.PreviousFireTime)
                     .Set(x => x.State, trigger.State)
                     //.Set(x => x.Type, trigger.Type)
                     .Set(x => x.StartTime, trigger.StartTime)
                     .Set(x => x.EndTime, trigger.EndTime)
                     .Set(x => x.CalendarName, trigger.CalendarName)
                     .Set(x => x.MisfireInstruction, trigger.MisfireInstruction)
                     .Set(x => x.Priority, trigger.Priority)
                     .Set(x => x.JobDataMap, trigger.JobDataMap);

        await Collection.UpdateOneAsync(filter, update);
    }

    public async Task<long> UpdateTriggerState(TriggerKey triggerKey, LocalTriggerState state)
    {
        // UPDATE TRIGGERS SET TRIGGER_STATE = @state WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, triggerKey.Name) &
                     FilterBuilder.Eq(x => x.Group, triggerKey.Group);

        var update = UpdateBuilder.Set(trigger => trigger.State, state);

        var result = await Collection.UpdateOneAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggerState(
        TriggerKey triggerKey,
        LocalTriggerState newState,
        LocalTriggerState oldState
    )
    {
        // UPDATE TRIGGERS
        // SET TRIGGER_STATE = @newState
        // WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup AND TRIGGER_STATE = @oldState";

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, triggerKey.Name) &
                     FilterBuilder.Eq(x => x.Group, triggerKey.Group) &
                     FilterBuilder.Eq(x => x.State, oldState);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateOneAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(
        GroupMatcher<TriggerKey> matcher,
        LocalTriggerState newState,
        params LocalTriggerState[] oldStates
    )
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression()) &
                     FilterBuilder.In(x => x.State, oldStates);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(
        JobKey jobKey,
        LocalTriggerState newState,
        params LocalTriggerState[] oldStates
    )
    {
        var filter = FilterBuilder.Where(x =>
            x.InstanceName == InstanceName && x.JobKey == jobKey && oldStates.Contains(x.State)
        );

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(JobKey jobKey, LocalTriggerState newState)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.JobKey.Group, jobKey.Group);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<long> UpdateTriggersStates(LocalTriggerState newState, params LocalTriggerState[] oldStates)
    {
        //var filter = FilterBuilder.Where(x => x.InstanceName == InstanceName && oldStates.Contains(x.State));

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & FilterBuilder.In(x => x.State, oldStates);

        var update = UpdateBuilder.Set(trigger => trigger.State, newState);

        var result = await Collection.UpdateManyAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task<long> DeleteTrigger(TriggerKey key)
    {
        // DELETE FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        var result = await Collection.DeleteOneAsync(filter);
        return result.DeletedCount;
    }

    public async Task<long> DeleteTriggers(JobKey jobKey)
    {
        // SELECT TRIGGER_NAME, TRIGGER_GROUP FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND JOB_NAME = @jobName AND JOB_GROUP = @jobGroup

        // DELETE FROM TRIGGERS WHERE SCHED_NAME = @schedulerName AND TRIGGER_NAME = @triggerName AND TRIGGER_GROUP = @triggerGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) & //
                     FilterBuilder.Eq(x => x.JobKey.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.JobKey.Group, jobKey.Group);

        var result = await Collection.DeleteManyAsync(filter);
        return result.DeletedCount;
    }

    /// <summary>
    /// Get the names of all of the triggers in the given state that have
    /// misfired - according to the given timestamp.  No more than count will
    /// be returned.
    /// </summary>
    /// <param name="nextFireTime"></param>
    /// <param name="maxResults"></param>
    /// <returns></returns>
    public async Task<(bool hasReachedLimit, List<TriggerKey> results)> HasMisfiredTriggers(
        DateTimeOffset nextFireTime,
        int maxResults
    )
    {
        // SELECT TRIGGER_NAME, TRIGGER_GROUP
        // FROM TRIGGERS
        // WHERE SCHED_NAME = @schedulerName AND MISFIRE_INSTR <> -1 AND NEXT_FIRE_TIME < @nextFireTime AND TRIGGER_STATE = @state1
        // ORDER BY NEXT_FIRE_TIME ASC, PRIORITY DESC

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Ne(x => x.MisfireInstruction, MisfireInstruction.IgnoreMisfirePolicy) &
                     FilterBuilder.Lt(x => x.NextFireTime, nextFireTime) &
                     FilterBuilder.Eq(x => x.State, LocalTriggerState.Waiting);

        var sort = SortBuilder.Ascending(trigger => trigger.NextFireTime)
                              .Descending(trigger => trigger.Priority);

        var cursor = await Collection
                           //
                           .Find(filter)
                           .Limit(maxResults + 1)
                           .Project(trigger => new TriggerKey(trigger.Name, trigger.Group))
                           .Sort(sort)
                           .ToCursorAsync();


        var results = new List<TriggerKey>();

        var hasReachedLimit = false;
        while (await cursor.MoveNextAsync() && !hasReachedLimit)
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

        return (hasReachedLimit, results);
    }
}
