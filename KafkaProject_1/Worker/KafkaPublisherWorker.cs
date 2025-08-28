using System.Text.Json;
using Zeebe.Client.Api.Responses;
using Zeebe.Client.Api.Worker;
using Confluent.Kafka;
using Zeebe.Client;

public class KafkaPublisherWorker : IHostedService


{

    private readonly IZeebeClient _client;
    private IJobWorker? _worker;

    public KafkaPublisherWorker(IZeebeClient client)
    {
        _client = client;
    }

    public Task StartAsync(CancellationToken stoppingToken)
    {
        _worker = _client.NewWorker()
            .JobType("publish-kafka")       // MUST match BPMN service task type
            .Handler(HandleJobAsync)
            .Name("KafkaPublisherWorker")
            .AutoCompletion()
            .MaxJobsActive(5)
             .PollInterval(TimeSpan.FromSeconds(1))
                .Timeout(TimeSpan.FromSeconds(10))
            .Open();

        // Keep the worker alive
        return Task.CompletedTask;

        
    }

    private  async Task HandleJobAsync(IJobClient jobClient, IJob job)
    {
        try
        {
            // Deserialize job.Variables (JSON) into a Dictionary
            var vars = JsonSerializer.Deserialize<Dictionary<string, object>>(job.Variables)
                       ?? new Dictionary<string, object>();

            var kafkaBootstrap = Get(vars, "kafkaBootstrap", "localhost:9092");
            var kafkaTopic = Get(vars, "kafkaTopic", "email-events");

            if (!vars.TryGetValue("email", out var emailObj))
                throw new InvalidOperationException("Process variable 'email' is required (object with to, subject, html).");

            // Serialize the 'email' object as message value
            var messageValue = emailObj is string s
                ? s // already JSON string
                : JsonSerializer.Serialize(emailObj);

            var config = new ProducerConfig
            {
                BootstrapServers = kafkaBootstrap,
                Acks = Acks.All,
                LingerMs = 5,
                EnableIdempotence = true
            };

            using var producer = new ProducerBuilder<string, string>(config).Build();

            // Use "to" field from email as key if available
            var key = ExtractEmailToOrDefault(emailObj, job.ProcessInstanceKey.ToString());

            Console.WriteLine($"[CamundaWorker] Producing to '{kafkaTopic}' -> key='{key}' value={messageValue}");

            var result = await producer.ProduceAsync(
                kafkaTopic,
                new Message<string, string> { Key = key, Value = messageValue }
            );

            Console.WriteLine($"[CamundaWorker] Job {job.Key} completed for process {job.ProcessInstanceKey}");


            await jobClient.NewCompleteJobCommand(job.Key)
                .Variables(JsonSerializer.Serialize(new { status = "Kafka message sent", topic = kafkaTopic }))
                .Send();
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CamundaWorker] ERROR: {ex.Message}");
            await jobClient.NewFailCommand(job.Key)
                .Retries(Math.Max(0, job.Retries - 1))
                .ErrorMessage(ex.Message)
                .Send();
        }
    }

    private static string Get(Dictionary<string, object> dict, string key, string fallback)
        => dict.TryGetValue(key, out var val) ? val?.ToString() ?? fallback : fallback;

    private static string ExtractEmailToOrDefault(object? emailObj, string fallback)
    {
        try
        {
            if (emailObj == null) return fallback;

            if (emailObj is string s)
            {
                var doc = JsonDocument.Parse(s);
                if (doc.RootElement.TryGetProperty("to", out var toEl))
                    return toEl.GetString() ?? fallback;
            }
            else if (emailObj is JsonElement el && el.ValueKind == JsonValueKind.Object)
            {
                if (el.TryGetProperty("to", out var toEl))
                    return toEl.GetString() ?? fallback;
            }
        }
        catch { /* ignore */ }
        return fallback;
    }



    public Task StopAsync(CancellationToken cancellationToken)
    {
        _worker?.Dispose();
        return Task.CompletedTask;
    }
}
