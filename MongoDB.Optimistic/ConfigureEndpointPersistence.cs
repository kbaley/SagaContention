using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using MongoDB.Driver;

class ConfigureEndpointPersistence : IConfigureEndpointTestExecution
{
    private const string databaseName = "AcceptanceTests";
    private IMongoClient client;
    public async Task Cleanup()
    {
        try {
            await client.DropDatabaseAsync(databaseName);
        }
        // ReSharper disable once EmptyGeneralCatchClause
        catch (Exception) { }
    }

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        var containerConnectionString = Environment.GetEnvironmentVariable("NServiceBusStorageMongoDB_ConnectionString");

        client = string.IsNullOrWhiteSpace(containerConnectionString) ? new MongoClient() : new MongoClient(containerConnectionString);

        configuration.UsePersistence<MongoPersistence>()
            .MongoClient(client)
            .DatabaseName(databaseName)
            .UseTransactions(false);

        return Task.FromResult(0);
    }
}
