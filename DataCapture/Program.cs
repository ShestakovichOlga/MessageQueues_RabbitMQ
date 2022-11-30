using RabbitMQ.Client;

namespace RabbitMQ
{
    class Program
    {
        static void Main(string[] args)
        {
            IModel connectionModel = new ConnectionFactory()
            {
                HostName = ServerOptions.HostName,
                UserName = ServerOptions.UserName,
                Password = ServerOptions.Password
            }
            .CreateConnection()
            .CreateModel();

            connectionModel.ExchangeDeclare(ServerOptions.ExchangeName, ExchangeType.Direct);
            connectionModel.QueueDeclare(
                queue: ServerOptions.QueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);
            connectionModel.QueueBind(ServerOptions.QueueName, ServerOptions.ExchangeName, ServerOptions.DirectexchangeKey);

            while (true)
            {
                string[] files = Directory.GetFiles(ServerOptions.InputPathFile, "*.pdf");

                foreach (var file in files)
                {
                    using (FileStream stream = File.OpenRead(file))
                    {
                        byte[] data = new byte[stream.Length];
                        stream.Read(data, 0, data.Length);
                        var batches = data.GetBatches();

                        var sequenceId = Guid.NewGuid().ToString();
                        var properties = connectionModel.CreateBasicProperties();
                        properties.Headers = new Dictionary<string, object> { { "sequenceId", sequenceId} };

                        foreach (var batch in batches)
                        {
                            properties.MessageId = batch.MessageId.ToString();
                            properties.Headers["size"] = batch.Size;
                            properties.Headers["position"] = batch.Position;
                            properties.Headers["isLast"] = batch.IsLast;
                            connectionModel.BasicPublish
                                (ServerOptions.ExchangeName,
                                ServerOptions.DirectexchangeKey,
                                properties,
                                batch.MessageBody);
                            Console.WriteLine($"Send message # {batch.Position} from sequence {sequenceId} file. Is last: {batch.IsLast}");
                        }
                    }
                    File.Delete(file);
                }
                Thread.Sleep(10000);
            }
        }
    }
}
