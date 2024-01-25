using System.Diagnostics.CodeAnalysis;

using JetBrains.Annotations;

using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using Quartz.Impl.Triggers;

namespace Quartz.Spi.MongoJobStore.Models;

[UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
internal class DailyTimeIntervalTrigger : Trigger
{
    public int RepeatCount { get; set; }

    [BsonRepresentation(BsonType.String)]
    public IntervalUnit RepeatIntervalUnit { get; set; }

    public int RepeatInterval { get; set; }

    public TimeOfDay StartTimeOfDay { get; set; }

    public TimeOfDay EndTimeOfDay { get; set; }

    public HashSet<DayOfWeek> DaysOfWeek { get; set; }

    public int TimesTriggered { get; set; }

    public string TimeZone { get; set; }


    public DailyTimeIntervalTrigger()
    {
    }

    [SetsRequiredMembers]
    public DailyTimeIntervalTrigger(IDailyTimeIntervalTrigger trigger, TriggerState state, string instanceName)
        : base(trigger, state, instanceName)
    {
        RepeatCount = trigger.RepeatCount;
        RepeatIntervalUnit = trigger.RepeatIntervalUnit;
        RepeatInterval = trigger.RepeatInterval;
        StartTimeOfDay = trigger.StartTimeOfDay;
        EndTimeOfDay = trigger.EndTimeOfDay;
        DaysOfWeek = new HashSet<DayOfWeek>(trigger.DaysOfWeek);
        TimesTriggered = trigger.TimesTriggered;
        TimeZone = trigger.TimeZone.Id;
    }

    public override ITrigger GetTrigger()
    {
        var trigger = new DailyTimeIntervalTriggerImpl
        {
            RepeatCount = RepeatCount,
            RepeatIntervalUnit = RepeatIntervalUnit,
            RepeatInterval = RepeatInterval,
            StartTimeOfDay = StartTimeOfDay,
            EndTimeOfDay = EndTimeOfDay,
            DaysOfWeek = new HashSet<DayOfWeek>(DaysOfWeek),
            TimesTriggered = TimesTriggered,
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
        };
        FillTrigger(trigger);
        return trigger;
    }
}
