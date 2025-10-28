using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore;

internal static class TriggerFactory
{
    public static Trigger CreateTrigger(ITrigger trigger, LocalTriggerState state, string instanceName)
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
