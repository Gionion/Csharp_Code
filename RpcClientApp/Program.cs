using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Threading.Tasks;

namespace RpcClientApp
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.Title = "RabbitMQ RPC Client";

            using (var rpcClient = new RpcClient())
            {
                Console.WriteLine("Press ENTER or Ctrl+C to exit.");

                while (true)
                {
                    string query = null;
                    string param1 = null;
                    string param2 = null;

                    Console.Write("Enter SQL Procedure to send: ");
                    query = Console.ReadLine();
                    Console.Write("Enter parameter 1: ");
                    param1 = Console.ReadLine();
                    Console.Write("Enter parameter 2: ");
                    param2 = Console.ReadLine();
                    string message = String.Format("EXEC dbo.{0} @Fecha_inicio = '{1}', @Fecha_fin = '{2}'", query, param1, param2);

                    if (string.IsNullOrWhiteSpace(message))
                        break;
                    else
                    {
                        var response = await rpcClient.SendAsync(message);
                        Console.Write($"Response was: {response} \n");
                    }
                }
            }
        }
    }
}

