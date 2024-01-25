using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson.Serialization.Attributes;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Calendar
{
    /// <summary>
    /// 
    /// </summary>
    /// <remarks>This is called sched_name</remarks>
    [BsonRequired]
    public required string InstanceName { get; set; }

    [BsonRequired]
    public required string CalendarName { get; set; }


    public byte[] Content { get; init; } // BSON Document


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
        return MongoDbJobStore.ObjectSerializer.DeSerialize<ICalendar>(Content);
    }
}
