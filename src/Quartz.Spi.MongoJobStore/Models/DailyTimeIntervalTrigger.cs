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

    public required TimeOfDay StartTimeOfDay { get; set; }

    [BsonIgnoreIfNull]
    public TimeOfDay? EndTimeOfDay { get; set; }

    public HashSet<DayOfWeek> DaysOfWeek { get; set; } = [];

    public int TimesTriggered { get; set; }

    public required string TimeZone { get; set; }


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

    public override IOperableTrigger GetTrigger()
    {
        var trigger = new DailyTimeIntervalTriggerImpl
        {
            RepeatCount = RepeatCount,
            RepeatIntervalUnit = RepeatIntervalUnit,
            RepeatInterval = RepeatInterval,
            // TODO: StartTimeOfDay = StartTimeOfDay ?? new TimeOfDay(0, 0, 0),
            StartTimeOfDay = StartTimeOfDay,
            EndTimeOfDay = EndTimeOfDay ?? new TimeOfDay(23, 59, 59),
            DaysOfWeek = new HashSet<DayOfWeek>(DaysOfWeek),
            TimesTriggered = TimesTriggered,
            TimeZone = TimeZoneInfo.FindSystemTimeZoneById(TimeZone),
        };
        FillTrigger(trigger);
        return trigger;
    }
}
