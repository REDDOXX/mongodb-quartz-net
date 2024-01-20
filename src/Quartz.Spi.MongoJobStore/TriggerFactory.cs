using Quartz.Spi.MongoJobStore.Models;

namespace Quartz.Spi.MongoJobStore;

internal static class TriggerFactory
{
    public static Trigger CreateTrigger(ITrigger trigger, Models.TriggerState state, string instanceName)
    {
        return trigger switch
        {
            ICronTrigger cronTrigger => new CronTrigger(cronTrigger, state, instanceName),
            ISimpleTrigger simpleTrigger => new SimpleTrigger(simpleTrigger, state, instanceName),
            ICalendarIntervalTrigger intervalTrigger => new CalendarIntervalTrigger(
                intervalTrigger,
                state,
                instanceName
            ),
            IDailyTimeIntervalTrigger intervalTrigger => new DailyTimeIntervalTrigger(
                intervalTrigger,
                state,
                instanceName
            ),
            _ => throw new NotSupportedException($"Trigger of type {trigger.GetType().FullName} is not supported")
        };
    }
}
