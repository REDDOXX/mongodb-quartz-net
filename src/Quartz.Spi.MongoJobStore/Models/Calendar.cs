using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Calendar
{
    [BsonId]
    public ObjectId Id { get; set; }

    /// <summary>
    /// 
    /// </summary>
    /// <remarks>This is called sched_name</remarks>
    [BsonRequired]
    public required string InstanceName { get; set; }

    [BsonRequired]
    public required string CalendarName { get; set; }

    public required byte[] Content { get; init; }


    public ICalendar CalContent { get; set; }


    public Calendar()
    {
    }

    [SetsRequiredMembers]
    public Calendar(string calendarName, ICalendar calendar, string instanceName)
    {
        InstanceName = instanceName;
        CalendarName = calendarName;

        Content = MongoDbJobStore.ObjectSerializer.Serialize(calendar);
    }

    public ICalendar GetCalendar()
    {
        var result = MongoDbJobStore.ObjectSerializer.DeSerialize<ICalendar>(Content);
        if (result == null)
        {
            throw new JobPersistenceException("Failed to deserialize calendar contents");
        }

        return result;
    }
}
