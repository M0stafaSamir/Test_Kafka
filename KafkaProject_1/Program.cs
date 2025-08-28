using CamundaKafkaDemo;
using Zeebe.Client;

namespace KafkaProject_1
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // 🔹 Connect to Zeebe
            var zeebeClient = ZeebeClient.Builder()
                .UseGatewayAddress("127.0.0.1:26500") // Zeebe gateway
                .UsePlainText()
                .Build();

            builder.Services.AddSingleton(zeebeClient);

            Console.WriteLine("✅ Connected to Zeebe");

            // 🔹 Register Kafka Producer Worker
            // Add background worker
            builder.Services.AddHostedService<KafkaPublisherWorker>();

            // 🔹 Add ASP.NET services
            builder.Services.AddControllers();
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            // 🔹 Register Kafka Consumer as background service
            builder.Services.AddHostedService<KafkaConsumerService>();

            var app = builder.Build();

            // 🔹 Swagger only in dev
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();
            app.UseAuthorization();
            app.MapControllers();

            Console.WriteLine("🚀 Application started. Workers + Consumer are running...");

            app.Run();
        }
    }
}
