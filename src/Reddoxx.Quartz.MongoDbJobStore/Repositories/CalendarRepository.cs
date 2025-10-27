using MongoDB.Driver;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class CalendarRepository : BaseRepository<Calendar>
{
    public CalendarRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "calendars", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<Calendar>(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.CalendarName),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
    }


    public async Task<bool> CalendarExists(string calendarName)
    {
        // SELECT 1 FROM CALENDARS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        return await Collection
                     //
                     .Find(filter)
                     .AnyAsync();
    }

    public async Task<ICalendar?> GetCalendar(string calendarName)
    {
        // SELECT CALENDAR FROM CALENDARS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        var result = await Collection
                           // 
                           .Find(filter)
                           .FirstOrDefaultAsync();

        return result?.GetCalendar();
    }

    public async Task<List<string>> GetCalendarNames()
    {
        // SELECT CALENDAR_NAME FROM CALENDARS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
                     //
                     .Distinct(calendar => calendar.CalendarName, filter)
                     .ToListAsync();
    }

    public async Task<long> GetCount()
    {
        // SELECT COUNT(CALENDAR_NAME)  FROM CALENDARS WHERE SCHED_NAME = @schedulerName
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
                     //
                     .Find(filter)
                     .CountDocumentsAsync();
    }

    public async Task AddCalendar(Calendar calendar)
    {
        await Collection.InsertOneAsync(calendar);
    }

    public async Task<long> UpdateCalendar(Calendar calendar)
    {
        // UPDATE CALENDARS
        // SET CALENDAR = @calendar
        // WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, calendar.InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendar.CalendarName);

        var update = UpdateBuilder.Set(x => x.Content, calendar.Content);

        var result = await Collection.UpdateOneAsync(filter, update);
        return result.MatchedCount;
    }

    public async Task<long> DeleteCalendar(string calendarName)
    {
        // DELETE FROM CALENDARS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        var result = await Collection.DeleteOneAsync(filter);

        return result.DeletedCount;
    }
}
