namespace RabbitMQ
{
    public static class ServerOptions
    {
        public static string QueueName => "demoQueue";

        public static string InputPathFile => @"C:\Input\";

        public static string OutputPathFile => @"C:\Output\";
        public static string HostName => "localhost";

        public static string UserName => "guest";

        public static string Password => "guest";

        public static string ExchangeName => "demoExchange";

        public static string DirectexchangeKey => "demodirectexchangekey";
    }
}