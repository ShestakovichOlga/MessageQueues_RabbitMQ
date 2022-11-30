using RabbitMQ.Client;
using System;

namespace RabbitMQ
{
    public class RabbitMQPublisher
    {
        private readonly IModel _connectionFactoryModel;

        public RabbitMQPublisher()
        {
            _connectionFactoryModel = new ConnectionFactory()
            {
                UserName = "guest",

                Password = "guest",

                HostName = "localhost"

            }.CreateConnection().CreateModel();
        }

        public void InitializeQueue(string exchangeName, string queueName, string directexchangeKey = "directexchange_key")
        {
            CreateExchange(exchangeName);
            CreateQueue(queueName);
            CreateBinding(exchangeName, queueName, directexchangeKey);
        }

        private void CreateExchange(string exchangeName)
        {
            _connectionFactoryModel.ExchangeDeclare(exchangeName, ExchangeType.Direct);


            Console.WriteLine("Created Exchange");
        }

        private void CreateQueue(string queueName)
        {

            _connectionFactoryModel.QueueDeclare(queue: queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            Console.WriteLine("Created queue");
        }

        private void CreateBinding(string exchangeName, string queueName, string directexchangeKey)
        {
            _connectionFactoryModel.QueueBind(queueName, exchangeName, directexchangeKey);

            Console.WriteLine("Created Binding");
        }

        public void PublishMessage(string fileName, IEnumerable<Batch> message)
        {
            var properties = _connectionFactoryModel.CreateBasicProperties();
            properties.Headers = new Dictionary<string, object> { { "sequenceId", fileName } };
            foreach (var batch in message)
            {
                properties.MessageId = batch.MessageId.ToString();
                properties.Headers["size"] = batch.Size;
                properties.Headers["position"] = batch.Position;
                properties.Headers["isLast"] = batch.IsLast;
                _connectionFactoryModel.BasicPublish
                    (ServerOptions.ExchangeName,
                    ServerOptions.DirectexchangeKey,
                    properties,
                    batch.MessageBody);
                Console.WriteLine($"Send message # {batch.Position} from {fileName} file. Is last: {batch.IsLast}");
            }
        }
    }
}