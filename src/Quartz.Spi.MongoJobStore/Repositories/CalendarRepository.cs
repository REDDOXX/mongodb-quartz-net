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

    public override Task EnsureIndex()
    {
        return Task.CompletedTask;
    }

    public async Task<bool> CalendarExists(string calendarName)
    {
        /*
         * SELECT 1 FROM CALENDARS
         * WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName
         */
        var filter = FilterBuilder.Where(calendar => calendar.Id == new CalendarId(calendarName, InstanceName));

        return await Collection.Find(filter).AnyAsync().ConfigureAwait(false);
    }

    public async Task<Calendar?> GetCalendar(string calendarName)
    {
        /*
          SELECT CALENDAR FROM CALENDARS WHERE SCHED_NAME = @schedulerName AND CALENDAR_NAME = @calendarName
         */

        return await Collection.Find(calendar => calendar.Id == new CalendarId(calendarName, InstanceName))
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetCalendarNames()
    {
        return await Collection.Distinct(
                calendar => calendar.Id.CalendarName,
                calendar => calendar.Id.InstanceName == InstanceName
            )
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetCount()
    {
        return await Collection.Find(calendar => calendar.Id.InstanceName == InstanceName)
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }

    public async Task AddCalendar(Calendar calendar)
    {
        await Collection.InsertOneAsync(calendar).ConfigureAwait(false);
    }

    public async Task<long> UpdateCalendar(Calendar calendar)
    {
        var result = await Collection.ReplaceOneAsync(cal => cal.Id == calendar.Id, calendar).ConfigureAwait(false);
        return result.MatchedCount;
    }

    public async Task<long> DeleteCalendar(string calendarName)
    {
        var result = await Collection
            .DeleteOneAsync(calendar => calendar.Id == new CalendarId(calendarName, InstanceName))
            .ConfigureAwait(false);
        return result.DeletedCount;
    }
}
