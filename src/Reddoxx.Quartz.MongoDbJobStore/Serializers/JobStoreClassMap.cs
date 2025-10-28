using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Serializers;

internal static class JobStoreClassMap
{
    public static void RegisterClassMaps()
    {
        BsonSerializer.RegisterGenericSerializerDefinition(typeof(ISet<>), typeof(SetSerializer<>));
        BsonSerializer.RegisterSerializer(new JobDataMapSerializer());

        BsonClassMap.RegisterClassMap<JobKey>(map =>
            {
                map.AutoMap();

                map.MapCreator(jobKey => new JobKey(jobKey.Name, jobKey.Group));
            }
        );

        BsonClassMap.RegisterClassMap<TriggerKey>(map =>
            {
                map.AutoMap();

                map.MapCreator(triggerKey => new TriggerKey(triggerKey.Name, triggerKey.Group));
            }
        );

        BsonClassMap.RegisterClassMap<TimeOfDay>(map =>
            {
                map.AutoMap();

                map.MapCreator(day => new TimeOfDay(day.Hour, day.Minute, day.Second));
            }
        );

        BsonClassMap.RegisterClassMap<JobDetail>(map =>
            {
                map.AutoMap();

                map.MapIdProperty(x => x.Id);

                map.MapProperty(x => x.Description)
                   .SetDefaultValue((string?)null);

                map.MapProperty(detail => detail.JobType)
                   .SetSerializer(new TypeSerializer());

                map.MapCreator(x => new JobDetail(
                        x.Id,
                        x.InstanceName,
                        x.Name,
                        x.Group,
                        x.Description,
                        x.JobType,
                        x.Durable,
                        x.ConcurrentExecutionDisallowed,
                        x.JobDataMap,
                        x.PersistJobDataAfterExecution,
                        x.RequestsRecovery
                    )
                );
            }
        );


        BsonClassMap.RegisterClassMap<Calendar>(map =>
            {
                map.AutoMap();

                map.MapIdProperty(x => x.Id);

                map.MapCreator(x => new Calendar(x.Id, x.InstanceName, x.CalendarName, x.Content));
            }
        );

        BsonClassMap.RegisterClassMap<FiredTrigger>(map =>
            {
                map.AutoMap();


                map.MapProperty(x => x.Fired)
                   .SetSerializer(new DateTimeOffsetSerializer(BsonType.DateTime));

                map.MapProperty(x => x.Scheduled)
                   .SetSerializer(NullableSerializer.Create(new DateTimeOffsetSerializer(BsonType.DateTime)));

                map.MapProperty(x => x.State)
                   .SetSerializer(new EnumSerializer<LocalTriggerState>(BsonType.String));


                map.MapCreator(x => new FiredTrigger(
                        x.Id,
                        x.InstanceName,
                        x.FiredInstanceId,
                        x.TriggerKey,
                        x.JobKey,
                        x.InstanceId,
                        x.Fired,
                        x.Scheduled,
                        x.Priority,
                        x.State,
                        x.ConcurrentExecutionDisallowed,
                        x.RequestsRecovery
                    )
                );
            }
        );

        BsonClassMap.RegisterClassMap<PausedTriggerGroup>(map =>
            {
                map.AutoMap();

                map.MapIdProperty(x => x.Id);

                map.MapCreator(x => new PausedTriggerGroup(x.Id, x.InstanceName, x.Group));
            }
        );

        BsonClassMap.RegisterClassMap<Scheduler>(map =>
            {
                map.AutoMap();

                map.MapProperty(x => x.LastCheckIn)
                   .SetSerializer(new DateTimeOffsetSerializer(BsonType.DateTime));

                map.MapCreator(x => new Scheduler(
                        //
                        x.Id,
                        x.SchedulerName,
                        x.InstanceId,
                        x.LastCheckIn,
                        x.CheckInInterval
                    )
                );
            }
        );

        BsonClassMap.RegisterClassMap<SchedulerLock>(map =>
            {
                map.AutoMap();

                map.MapProperty(x => x.LockType)
                   .SetSerializer(new EnumSerializer<QuartzLockType>(BsonType.String));

                map.MapCreator(x => new SchedulerLock(x.Id, x.InstanceName, x.LockType, x.LockKey));
            }
        );

        RegisterTriggerClassMaps();
    }

    private static void RegisterTriggerClassMaps()
    {
        BsonClassMap.RegisterClassMap<Trigger>(map =>
            {
                map.AutoMap();

                map.MapIdProperty(x => x.Id);

                map.MapProperty(x => x.Description)
                   .SetDefaultValue((string?)null);

                map.MapProperty(x => x.NextFireTime)
                   .SetDefaultValue((DateTimeOffset?)null)
                   .SetSerializer(NullableSerializer.Create(new DateTimeOffsetSerializer(BsonType.DateTime)));

                map.MapProperty(x => x.PreviousFireTime)
                   .SetDefaultValue((DateTimeOffset?)null)
                   .SetSerializer(NullableSerializer.Create(new DateTimeOffsetSerializer(BsonType.DateTime)));

                map.MapProperty(x => x.State)
                   .SetSerializer(new EnumSerializer<LocalTriggerState>(BsonType.String));

                map.MapProperty(x => x.StartTime)
                   .SetSerializer(new DateTimeOffsetSerializer(BsonType.DateTime));

                map.MapProperty(x => x.EndTime)
                   .SetDefaultValue((DateTimeOffset?)null)
                   .SetSerializer(NullableSerializer.Create(new DateTimeOffsetSerializer(BsonType.DateTime)));

                map.MapProperty(x => x.CalendarName)
                   .SetDefaultValue((string?)null);

                map.SetIsRootClass(isRootClass: true);
            }
        );

        BsonClassMap.RegisterClassMap<CronTrigger>(map =>
            {
                map.AutoMap();

                map.MapCreator(x => new CronTrigger(
                        x.Id,
                        x.InstanceName,
                        x.Name,
                        x.Group,
                        x.Description,
                        x.NextFireTime,
                        x.PreviousFireTime,
                        x.State,
                        x.StartTime,
                        x.EndTime,
                        x.CalendarName,
                        x.MisfireInstruction,
                        x.Priority,
                        x.JobDataMap,
                        x.JobKey,
                        //
                        x.CronExpression,
                        x.TimeZone
                    )
                );
            }
        );

        BsonClassMap.RegisterClassMap<SimpleTrigger>(map =>
            {
                map.AutoMap();

                map.MapCreator(x => new SimpleTrigger(
                        x.Id,
                        x.InstanceName,
                        x.Name,
                        x.Group,
                        x.Description,
                        x.NextFireTime,
                        x.PreviousFireTime,
                        x.State,
                        x.StartTime,
                        x.EndTime,
                        x.CalendarName,
                        x.MisfireInstruction,
                        x.Priority,
                        x.JobDataMap,
                        x.JobKey,
                        //
                        x.RepeatCount,
                        x.RepeatInterval,
                        x.TimesTriggered
                    )
                );
            }
        );

        BsonClassMap.RegisterClassMap<CalendarIntervalTrigger>(map =>
            {
                map.AutoMap();

                map.MapProperty(x => x.RepeatIntervalUnit)
                   .SetSerializer(new EnumSerializer<IntervalUnit>(BsonType.String));

                map.MapCreator(x => new CalendarIntervalTrigger(
                        x.Id,
                        x.InstanceName,
                        x.Name,
                        x.Group,
                        x.Description,
                        x.NextFireTime,
                        x.PreviousFireTime,
                        x.State,
                        x.StartTime,
                        x.EndTime,
                        x.CalendarName,
                        x.MisfireInstruction,
                        x.Priority,
                        x.JobDataMap,
                        x.JobKey,
                        //
                        x.RepeatIntervalUnit,
                        x.RepeatInterval,
                        x.TimesTriggered,
                        x.TimeZone,
                        x.PreserveHourOfDayAcrossDaylightSavings,
                        x.SkipDayIfHourDoesNotExist
                    )
                );
            }
        );

        BsonClassMap.RegisterClassMap<DailyTimeIntervalTrigger>(map =>
            {
                map.AutoMap();


                map.MapProperty(x => x.RepeatIntervalUnit)
                   .SetSerializer(new EnumSerializer<IntervalUnit>(BsonType.String));

                map.MapProperty(x => x.EndTimeOfDay)
                   .SetDefaultValue((TimeOfDay?)null);

                map.MapProperty(trigger => trigger.DaysOfWeek)
                   .SetSerializer(
                       new EnumerableInterfaceImplementerSerializer<HashSet<DayOfWeek>, DayOfWeek>(
                           new EnumSerializer<DayOfWeek>(BsonType.String)
                       )
                   );


                map.MapCreator(x => new DailyTimeIntervalTrigger(
                        x.Id,
                        x.InstanceName,
                        x.Name,
                        x.Group,
                        x.Description,
                        x.NextFireTime,
                        x.PreviousFireTime,
                        x.State,
                        x.StartTime,
                        x.EndTime,
                        x.CalendarName,
                        x.MisfireInstruction,
                        x.Priority,
                        x.JobDataMap,
                        x.JobKey,
                        // 
                        x.RepeatCount,
                        x.RepeatIntervalUnit,
                        x.RepeatInterval,
                        x.StartTimeOfDay,
                        x.EndTimeOfDay,
                        x.DaysOfWeek,
                        x.TimesTriggered,
                        x.TimeZone
                    )
                );
            }
        );
    }
}
