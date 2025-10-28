using JetBrains.Annotations;

using MongoDB.Bson;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class Calendar
{
    public ObjectId Id { get; init; }

    /// <summary>
    /// </summary>
    /// <remarks>This is called sched_name</remarks>
    public string InstanceName { get; init; }

    public string CalendarName { get; init; }

    public byte[] Content { get; init; }


    public Calendar(ObjectId id, string instanceName, string calendarName, byte[] content)
    {
        Id = id;
        InstanceName = instanceName;
        CalendarName = calendarName;
        Content = content;
    }

    public Calendar(string calendarName, ICalendar calendar, string instanceName)
    {
        Id = ObjectId.GenerateNewId();
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
