namespace RabbitMQ
{
    public class Batch
    {
        public Guid MessageId { get; set; }

        public int Position { get; set; }

        public int Size { get; set; }

        public byte[]? MessageBody { get; set; }

        public bool IsLast { get; set; }
    }
}
