using System;
using System.Threading.Tasks;
using NServiceBus;
using NServiceBus.AcceptanceTesting.Support;
using NServiceBus.Persistence.Sql;
using System.Data.SqlClient;

class ConfigureEndpointPersistence : IConfigureEndpointTestExecution
{
    public Task Cleanup()
    {
        return Task.CompletedTask;
    }

    public Task Configure(string endpointName, EndpointConfiguration configuration, RunSettings settings, PublisherMetadata publisherMetadata)
    {
        var containerConnectionString = @"Data Source=localhost;Initial Catalog=nsb;Integrated Security=True";

        var replacementConnectionString = Environment.GetEnvironmentVariable("NServiceBus_SagaContention");
        if (!string.IsNullOrEmpty(replacementConnectionString))
            containerConnectionString = replacementConnectionString;

        var persistence = configuration.UsePersistence<SqlPersistence>();
        persistence.SqlDialect<SqlDialect.MsSqlServer>();
        persistence.ConnectionBuilder(
            connectionBuilder: () => new SqlConnection(containerConnectionString));
        var subscriptions = persistence.SubscriptionSettings();
        subscriptions.DisableCache();

        return Task.FromResult(0);
    }
}
