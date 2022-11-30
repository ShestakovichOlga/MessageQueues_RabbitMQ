namespace RabbitMQ
{
    internal static class BatchingService
    {
        public static IEnumerable<Batch> GetBatches(this byte[] data)
        {
            var maxBatchSize = 1024;

            var sequenceId = Guid.NewGuid();

            if (data.Length <= maxBatchSize)
            {
                yield return new Batch
                {
                    MessageId = sequenceId,
                    Position = 0,
                    Size = data.Length,
                    MessageBody = data,
                    IsLast = true
                };
            }

            var size = (int)Math.Ceiling((double)data.Length / maxBatchSize);

            for (var i = 0; i < size; i++)
            {
                var bodyLength = i != size - 1 ? maxBatchSize : data.Length % maxBatchSize;

                var messageBody = new byte[bodyLength];

                Array.Copy(data, maxBatchSize * i, messageBody, 0, bodyLength);

                yield return new Batch
                {
                    MessageId = sequenceId,
                    Position = i,
                    Size = size,
                    MessageBody = messageBody,
                    IsLast = i + 1 == size
                };
            }
        }
    }
}
