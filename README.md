MongoDB Job Store for Quartz.NET
================================

[![NuGet Version](https://img.shields.io/nuget/v/Reddoxx.Quartz.MongoDbJobStore)](https://www.nuget.org/packages/Reddoxx.Quartz.MongoDbJobStore)

Fork of the awesome codebase of [@glucaci](https://github.com/glucaci/mongodb-quartz-net) with multiple tweaks:

- Latest .net support
- Quartz cluster support
- DI-based configuration
- Improved locking support

## Limitations
- Due to the nature of the DI-based approach multiple schedulers are not supported on the same host (`SchedulerBuilder.Build()`).
- The locking mechanism has been rebuilt to use a `SELECT FOR UPDATE` approach, which requires transactions. 
So your MongoDb needs to run in a **replica-set** configuration. We also provide a way to use redis instead of mongodb 
transactions for locking (using the redlock algorithm).
 
## Nuget

```
Install-Package Reddoxx.Quartz.MongoDbJobStore
```

## Basic Usage

First, create your own `QuartzMongoDbJobStoreFactory`, which provides the a database-instance where your collection should be stored in. 
The JobStoreFactory itself also needs to be added to your DI-Container as it'll be resolved by the `MongoDbJobStore` later on.

```csharp
internal class QuartzMongoDbJobStoreFactory : IQuartzMongoDbJobStoreFactory
{
    private const string LocalConnectionString = "mongodb://localhost/quartz";

    private readonly IMongoDatabase _database;

    public QuartzMongoDbJobStoreFactory()
    {
        var url = new MongoUrl(LocalConnectionString);
        var client = new MongoClient(url);

        _database = client.GetDatabase(url.DatabaseName);
    }

    public IMongoDatabase GetDatabase()
    {
        return _database;
    }
}
```

Next, register your `QuartzMongoDbJobStoreFactory` in your DI-Container:

```csharp
    // Make your job store factory available to the MongoDbJobStore
    services.AddSingleton<IQuartzMongoDbJobStoreFactory, QuartzMongoDbJobStoreFactory>();
```

Then we can configure quartz for the host. Be sure to specify `MongoDbJobStore` in `q.UsePersistentStore<MongoDbJobStore>` as this 
registers the `MongoDbJobStore` singleton as well. `storage.ConfigureMongoDb(c => ...)` allows for further customization.

```csharp
    services.AddQuartz(
        q =>
        {
            q.SchedulerId = "AUTO";

            q.UsePersistentStore<MongoDbJobStore>(
                storage =>
                {
                    storage.UseClustering();
                    storage.UseNewtonsoftJsonSerializer();
    
                    // Your custom job store configuration
                    storage.ConfigureMongoDb(
                        c =>
                        {
                            // Configure your custom collection prefix
                            c.CollectionPrefix = "CustomPrefix";
                        }
                    );
                }
            );
        }
    );
```

## Use redlock for locking instead of mongodb transactions

```shell
Install-Package Reddoxx.Quartz.MongoDbJobStore.Redlock
```

```csharp
    // Add the DistributedLocksQuartzLockingManager to your DI container
    services.AddSingleton<IQuartzJobStoreLockingManager, DistributedLocksQuartzLockingManager>();
```