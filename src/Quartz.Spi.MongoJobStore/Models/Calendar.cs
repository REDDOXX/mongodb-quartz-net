using MongoDB.Bson.Serialization.Attributes;

using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Models;

internal class Calendar
{
    [BsonId]
    public CalendarId Id { get; set; }

    public byte[] Content { get; set; }


    public Calendar()
    {
    }

    public Calendar(string calendarName, ICalendar calendar, string instanceName)
    {
        Id = new CalendarId(calendarName, instanceName);
        Content = MongoDbJobStore.ObjectSerializer.Serialize(calendar);
    }

    public ICalendar GetCalendar()
    {
        return MongoDbJobStore.ObjectSerializer.DeSerialize<ICalendar>(Content);
    }
}
