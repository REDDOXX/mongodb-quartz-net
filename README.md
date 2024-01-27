MongoDB Job Store for Quartz.NET
================================
Thanks to @chrisdrobison for handing over this project.

## Nuget

```
Install-Package Quartz.Spi.MongoJobStore
```

## Basic Usage

First, create a MongoDb database factory and add it to your DI-container.

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

```csharp
    services.AddSingleton<IMongoDbJobStoreConnectionFactory, LocalMongoDbJobStoreConnectionFactory>();
```

Then, simply `UsePersistentStore` with `MongoDbJobStore`:

```csharp
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