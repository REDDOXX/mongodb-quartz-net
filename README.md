MongoDB Job Store for Quartz.NET
================================
Fork of the awesome codebase of [@glucaci](https://github.com/glucaci/mongodb-quartz-net) with multiple tweaks:

- Latest .net support
- Quartz cluster support
- DI-based configuration

## Nuget

```
Install-Package Quartz.Spi.MongoJobStore
```

## Basic Usage

To use the MongoDb job-store you need to add your custom `IMongoDbJobStoreConnectionFactory` to the DI-container
and configure the `UsePersistentStore` with `MongoDbJobStore`.

```csharp
    // MongoDb connection factory
    services.AddSingleton<IMongoDbJobStoreConnectionFactory, LocalMongoDbJobStoreConnectionFactory>();

    services.AddQuartz(
        q =>
        {
            q.SchedulerId = "AUTO";

            q.UsePersistentStore<MongoDbJobStore>(
                storage =>
                {
                    storage.UseClustering();
            
                    // Enable json serializers (required)
                    storage.UseNewtonsoftJsonSerializer();
    
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

The connection factory is required to return the database instance for your collections.
```csharp
internal class LocalMongoDbJobStoreConnectionFactory : IMongoDbJobStoreConnectionFactory
{
    private const string LocalConnectionString = "mongodb://localhost/quartz?minPoolSize=16&maxConnecting=32";

    private readonly IMongoDatabase _database;

    public LocalMongoDbJobStoreConnectionFactory()
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
