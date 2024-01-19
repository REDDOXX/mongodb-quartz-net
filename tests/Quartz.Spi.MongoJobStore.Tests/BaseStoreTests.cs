﻿using System.Collections.Specialized;

using Quartz.Impl;

namespace Quartz.Spi.MongoJobStore.Tests;

public abstract class BaseStoreTests
{
    public const string Barrier = "BARRIER";
    public const string DateStamps = "DATE_STAMPS";
    public static readonly TimeSpan TestTimeout = TimeSpan.FromSeconds(125);

    protected static async Task<IScheduler> CreateScheduler(string instanceName = "QUARTZ_TEST")
    {
        var properties = new NameValueCollection
        {
            ["quartz.serializer.type"] = "binary",
            [StdSchedulerFactory.PropertySchedulerInstanceName] = instanceName,
            [StdSchedulerFactory.PropertySchedulerInstanceId] = $"{Environment.MachineName}-{Guid.NewGuid()}",
            [StdSchedulerFactory.PropertyJobStoreType] = typeof(MongoDbJobStore).AssemblyQualifiedName,
            [$"{StdSchedulerFactory.PropertyJobStorePrefix}.{StdSchedulerFactory.PropertyDataSourceConnectionString}"] =
                "mongodb://localhost/quartz",
            [$"{StdSchedulerFactory.PropertyJobStorePrefix}.collectionPrefix"] = "prefix",
        };

        var scheduler = new StdSchedulerFactory(properties);
        return await scheduler.GetScheduler();
    }
}
