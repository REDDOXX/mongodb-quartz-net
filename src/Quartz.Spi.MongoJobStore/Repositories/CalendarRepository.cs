using MongoDB.Driver;

using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

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
                Builders<Calendar>.IndexKeys.Combine(
                    Builders<Calendar>.IndexKeys.Ascending(x => x.InstanceName),
                    Builders<Calendar>.IndexKeys.Ascending(x => x.CalendarName)
                ),
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
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task<Calendar?> GetCalendar(string calendarName)
    {
        // SELECT CALENDAR FROM CALENDARS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        return await Collection
            // 
            .Find(filter)
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetCalendarNames()
    {
        // SELECT CALENDAR_NAME FROM CALENDARS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
            //
            .Distinct(calendar => calendar.CalendarName, filter)
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

    public async Task AddCalendar(Calendar calendar)
    {
        await Collection.InsertOneAsync(calendar).ConfigureAwait(false);
    }

    public async Task<long> UpdateCalendar(Calendar calendar)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, calendar.InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendar.CalendarName);

        var result = await Collection.ReplaceOneAsync(filter, calendar).ConfigureAwait(false);
        return result.MatchedCount;
    }

    public async Task<long> DeleteCalendar(string calendarName)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.CalendarName, calendarName);

        var result = await Collection
            //
            .DeleteOneAsync(filter)
            .ConfigureAwait(false);

        return result.DeletedCount;
    }
}
