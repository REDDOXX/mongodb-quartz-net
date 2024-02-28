using JetBrains.Annotations;

namespace Reddoxx.Quartz.MongoDbJobStore.Tests.Options;

[PublicAPI]
public class RedisOptions
{
    public required string Host { get; set; }

    public required int Port { get; set; }
}
