using Confluent.Kafka;
using Microsoft.VisualBasic;

namespace WebAPI.B.BaseHostedService;

public class BaseHostedService : IHostedService
{
    private readonly ILogger<BaseHostedService> _logger;
    private readonly ConsumerConfig _consumerConfig;
    private readonly AdminClientConfig _adminClientConfig;

    public BaseHostedService(ILogger<BaseHostedService> logger)
    {
        _logger = logger;
        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "foo",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            AllowAutoCreateTopics = true
        };
        _adminClientConfig = new AdminClientConfig
        {
            BootstrapServers = "localhost:9092"
        };
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Timed Hosted Service running");

        new Thread(() => StartConsumeTopics(cancellationToken)).Start();

        return Task.CompletedTask;

    }

    private void StartConsumeTopics(CancellationToken cancellationToken)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();

        consumer.Subscribe(ObjectOfValue.Constants.TopicA);

        while (!cancellationToken.IsCancellationRequested)
        {
            if(!IsTopicAvailable()) continue;
            var topic = consumer.Consume(cancellationToken);
            _logger.LogInformation("Received data on {topic}: {value}", ObjectOfValue.Constants.TopicA, topic.Message.Value);
        }

        consumer.Close();
    }

    private bool IsTopicAvailable()
    {
        using var adminClient = new AdminClientBuilder(_adminClientConfig).Build();
        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        return metadata.Topics.Any(t =>
            t.Topic.Equals(ObjectOfValue.Constants.TopicA, StringComparison.InvariantCultureIgnoreCase));
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _logger.LogInformation("Timed Hosted Service stopped");
        return Task.CompletedTask;
    }
}