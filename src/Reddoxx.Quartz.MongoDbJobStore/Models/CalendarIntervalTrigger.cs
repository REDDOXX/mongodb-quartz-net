using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Quartz;
using Quartz.Impl.Triggers;
using Quartz.Spi;

namespace Reddoxx.Quartz.MongoDbJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class CalendarIntervalTrigger : Trigger
{
    [BsonRepresentation(BsonType.String)]
    public IntervalUnit RepeatIntervalUnit { get; set; }

    public int RepeatInterval { get; set; }

    public int TimesTriggered { get; set; }

    public required string TimeZone { get; set; }

    public bool PreserveHourOfDayAcrossDaylightSavings { get; set; }

    public bool SkipDayIfHourDoesNotExist { get; set; }


    public CalendarIntervalTrigger()
    {
    }

    [SetsRequiredMembers]
    public CalendarIntervalTrigger(ICalendarIntervalTrigger trigger, TriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        RepeatIntervalUnit = trigger.RepeatIntervalUnit;
        RepeatInterval = trigger.RepeatInterval;
        TimesTriggered = trigger.TimesTriggered;
        TimeZone = trigger.TimeZone.Id;
        PreserveHourOfDayAcrossDaylightSavings = trigger.PreserveHourOfDayAcrossDaylightSavings;
        SkipDayIfHourDoesNotExist = trigger.SkipDayIfHourDoesNotExist;
    }

    public override IOperableTrigger GetTrigger()
    {
        var trigger = new CalendarIntervalTriggerImpl
        {
            RepeatIntervalUnit = RepeatIntervalUnit,
            RepeatInterval = RepeatInterval,
            TimesTriggered = TimesTriggered,
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
            PreserveHourOfDayAcrossDaylightSavings = PreserveHourOfDayAcrossDaylightSavings,
            SkipDayIfHourDoesNotExist = SkipDayIfHourDoesNotExist,
        };
        FillTrigger(trigger);
        return trigger;
    }
}
