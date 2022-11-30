using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Collections.Concurrent;
using System.Text;

namespace RabbitMQ
{
    class Program
    {
        private static readonly ConcurrentDictionary<string, List<Batch>> _data = new();

        static void Main(string[] args)
        {
        var connectionFactory = new ConnectionFactory()
            {
                HostName = ServerOptions.HostName,
                UserName = ServerOptions.UserName,
                Password = ServerOptions.Password
            };

            using var connection = connectionFactory.CreateConnection();
            using var channel = connection.CreateModel();
            channel.QueueDeclare(
                queue: ServerOptions.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            var consumer = new EventingBasicConsumer(channel);

            consumer.Received += ReceivedBatches;

            channel.BasicConsume(
                queue: ServerOptions.QueueName,
                autoAck: true,
                consumer: consumer);

            Console.WriteLine("The consumer subscribed to the queue");
            Console.ReadKey();
        }

        private static void ReceivedBatches(object sender, BasicDeliverEventArgs args)
        {
            var size = (int)args.BasicProperties.Headers["size"];
            var position = (int)args.BasicProperties.Headers["position"];
            var isLast = (bool)args.BasicProperties.Headers["isLast"];
            _data.GetOrAdd(args.BasicProperties.MessageId, new List<Batch>());

            var batch = new Batch
            {
                MessageBody = args.Body.ToArray(),
                Position = position,
                Size = size,
                IsLast = isLast
            };

            _data[args.BasicProperties.MessageId].Add(batch);

            if (_data[args.BasicProperties.MessageId].Count == size && batch.IsLast == true)
            {
                var fileName = $"File_{Encoding.UTF8.GetString((byte[])args.BasicProperties.Headers["sequenceId"])}.pdf";
                var bytes = new List<byte>();
                foreach (var data in _data[args.BasicProperties.MessageId].OrderBy(x => x.Position))
                {
                    bytes.AddRange(data.MessageBody);
                }
                File.WriteAllBytes(ServerOptions.OutputPathFile + fileName, bytes.ToArray());

                Console.WriteLine($"New {fileName} recieved.");
            }
        }
    }
}
