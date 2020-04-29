using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Rebus.Activation;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Logging;
using Rebus.Persistence.InMem;

namespace Rebus.RabbitMq.Bug
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var handlerActivator = new BuiltinHandlerActivator();
            handlerActivator.Register(() => new TestDrainer());
            var drainer = SetupQueue("RebusTestBug",handlerActivator).Start();
            drainer.Subscribe<TestType>();
            
            var publisher = SetupQueue("RebusTestBug.Publisher", new BuiltinHandlerActivator()).Start();
            
            var testsender = new Tester(publisher);

            for (int i = 0; i < 100; i++)
            {
                var payload = new TestType(){Data = $"test {i}"};
                await testsender.TestRetry(payload);
                await Task.Delay(1000);
            }
        }

        public static RebusConfigurer SetupQueue(string queueName, IContainerAdapter handler)
        {
            ConfigurationBuilder configurationBuilder = new ConfigurationBuilder();

            var configuration = configurationBuilder
                .AddJsonFile("appsettings.json")
                .Build();
            var connectionString = configuration.GetConnectionString("MessageBus");
            return Configure.With(handler)
                .Timeouts( t => t.StoreInMemory())
                .Transport(t => t.UseRabbitMq(connectionString, queueName)
                                                            .EnablePublisherConfirms(true)
                                                            .ConfigureSsl(connectionString))
                .Logging(c => c.ColoredConsole());
        }
    }

    internal class TestDrainer:Rebus.Handlers.IHandleMessages<TestType>
    {
        public Task Handle(TestType message)
        {
            Console.WriteLine($"Received {message.Data}");
            return Task.CompletedTask;
        }
    }

    public class TestType
    {
        public string Data { get; set; }
    }

    internal class Tester
    {
        private readonly IBus _bus;
        public Tester(IBus bus)
        {
            _bus = bus;
        }
        
        public async Task TestRetry(object payload)
        {
            try
            {
                await _bus.Publish(payload);
            }
            catch (Exception e)
            {
                new ConsoleLoggerFactory(true).GetLogger<Tester>().Error($"Error, retrying: {e.Message}");
                await Task.Delay(500);
                await TestRetry(payload);
            }
        }
    }

    internal static class TestExtensions
    {
        public static RabbitMqOptionsBuilder ConfigureSsl(this RabbitMqOptionsBuilder builder, string connectionString)
        {
            var uri = new Uri(connectionString);
            if (uri.Scheme.Equals("amqps", StringComparison.InvariantCultureIgnoreCase))
            {
                var sslSettings = new SslSettings(true, uri.Host);
                builder.Ssl(sslSettings);
            }

            return builder;
        }
    }
}