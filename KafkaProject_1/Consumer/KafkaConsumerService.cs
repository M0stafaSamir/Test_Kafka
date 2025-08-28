using Confluent.Kafka;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace CamundaKafkaDemo
{
    public class KafkaConsumerService : BackgroundService
    {
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly string _topic = "process-topic";
        private readonly IConsumer<Ignore, string> _consumer;

        public KafkaConsumerService(ILogger<KafkaConsumerService> logger)
        {
            _logger = logger;

            var config = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "camunda-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            _consumer.Subscribe(_topic);

            _logger.LogInformation("Kafka Consumer started, listening to topic: {Topic}", _topic);

            try
            {
                while (!stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        var result = _consumer.Consume(stoppingToken);

                        if (result?.Message != null)
                        {
                            _logger.LogInformation("---------------------------");

                            _logger.LogInformation("📩 Received message: {Message}", result.Message.Value);

                            _logger.LogInformation("---------------------------");



                            // TODO: Instead of log, send email here
                            // e.g., EmailService.Send(result.Message.Value);

                        }
                    }
                    catch (ConsumeException ex)
                    {
                        _logger.LogError(ex, "Error while consuming Kafka message.");
                    }

                    await Task.Delay(500, stoppingToken);
                }
            }
            catch (OperationCanceledException)
            {
                _logger.LogInformation("Kafka Consumer shutting down...");
            }
            finally
            {
                _consumer.Close();
            }
        }
    }
}
