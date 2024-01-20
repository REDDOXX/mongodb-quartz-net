namespace Quartz.Spi.MongoJobStore.Models.Id;

internal class CalendarId
{
    public string InstanceName { get; set; }

    public string CalendarName { get; set; }


    public CalendarId()
    {
    }

    public CalendarId(string calendarName, string instanceName)
    {
        InstanceName = instanceName;
        CalendarName = calendarName;
    }
}
