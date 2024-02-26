using JetBrains.Annotations;

using Microsoft.Extensions.DependencyInjection;

using Reddoxx.Quartz.MongoDbJobStore.Database;

namespace Reddoxx.Quartz.MongoDbJobStore.Extensions;

[PublicAPI]
public static class ServiceCollectionExtension
{
    public static IServiceCollection AddQuartzMongoDb(this IServiceCollection services)
    {
        //services.AddSingleton<IQuartzMongoDbJobStoreFactory, QuartzMongoDbJobStoreFactory>();

        return services;
    }
}
