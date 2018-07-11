using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace RpcServerApp
{
    class Program
    {
        private static IModel channel;

        static void Main(string[] args)
        {
            Console.Title = "RabbitMQ RPC Server";

            var factory = new ConnectionFactory() { HostName = "localhost", Port = 5672, UserName = "guest", Password = "guest" };

            using (var connection = factory.CreateConnection())
            {
                using (channel = connection.CreateModel())
                {
                    const string requestQueueName = "requestqueue";
                    channel.QueueDeclare(requestQueueName, true, false, false, null);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += Consumer_Received;
                    channel.BasicConsume(requestQueueName, true, consumer);

                    Console.WriteLine("Waiting for messages...");
                    Console.WriteLine("Press ENTER to exit.");
                    Console.WriteLine();
                    Console.ReadLine();
                }
            }
        }

        private static void Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            var requestMessage = Encoding.UTF8.GetString(e.Body);
            var correlationId = e.BasicProperties.CorrelationId;
            string responseQueueName = e.BasicProperties.ReplyTo;
            string responseMessage = "";
            Procedure_SQL(requestMessage, correlationId, responseQueueName, responseMessage);
        }

        public static void Procedure_SQL(string requestMessage, string correlationId, string responseQueueName, string responseMessage)
        {
            string ConnectionString = @"Data Source = host_name\Instance_name; user id=user; password=pass; Initial Catalog = DB_name";
            SqlConnection DBConnection = new SqlConnection(ConnectionString);

            DBConnection.Open();

            string query = requestMessage;
            
            SqlCommand queryCommand = new SqlCommand(query, DBConnection);

            SqlDataReader queryCommandReader = queryCommand.ExecuteReader();

            DataTable dataTable = new DataTable();

            dataTable.Load(queryCommandReader);

            String columns = string.Empty;
            String sql1 = string.Empty;
            foreach (DataColumn column in dataTable.Columns)
            {
                columns += column.ColumnName + " | ";
            }
            sql1 = columns;

            for (int i = 0; i < dataTable.Rows.Count; i++)
            {
                String rowText = string.Empty;
                foreach (DataColumn column in dataTable.Columns)
                {
                    rowText += dataTable.Rows[i][column.ColumnName] + " | ";
                }
                sql1 += rowText;
            }
            responseMessage = sql1;
            DBConnection.Close();
            Send_Data(requestMessage, correlationId, responseQueueName, responseMessage);
        }

        private static void Send_Data(string requestMessage, string correlationId, string responseQueueName, string responseMessage)
        {

            Console.WriteLine($"Received: {requestMessage} with CorrelationId {correlationId}");
            Publish(responseMessage, correlationId, responseQueueName);
        }

        private static void Publish(string responseMessage, string correlationId, string responseQueueName)
        {
            byte[] responseMessageBytes = Encoding.UTF8.GetBytes(responseMessage);

            const string exchangeName = ""; // default exchange
            var responseProps = channel.CreateBasicProperties();
            responseProps.CorrelationId = correlationId;

            channel.BasicPublish(exchangeName, responseQueueName, responseProps, responseMessageBytes);

            //Console.WriteLine($"Sent: {responseMessage} with CorrelationId {correlationId}");
            Console.WriteLine($"Sent: {responseMessage}");
            Console.WriteLine();
        }
    }
}
