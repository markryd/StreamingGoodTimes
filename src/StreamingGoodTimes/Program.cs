using System;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Sockets;
using System.Runtime.Serialization.Formatters;
using System.Threading;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Bson;
using Newtonsoft.Json.Serialization;

namespace StreamingGoodTimes
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var serverTask = Task.Run(() => RunServer());
            var clientTask = Task.Run(() => RunClient());

            Task.WhenAll(serverTask, clientTask).GetAwaiter().GetResult();
            Console.WriteLine("All done");
            Console.ReadKey();
        }

        const int NumWrites = 10;
        const int MessageLength = 4096;

        private static void RunServer()
        {
            var serializer = GetSerializer();
            var listener = new TcpListener(IPAddress.Parse("127.0.0.1"), 27111);
            try
            {
                listener.Start(1);

                using (var server = listener.AcceptTcpClientAsync().GetAwaiter().GetResult())
                using (var stream = server.GetStream())
                {
                    for (var j = 0; j < NumWrites; j++)
                        using (var zip = new DeflateStream(stream, CompressionMode.Compress, true))
                        using (var buffer = new BufferedStream2(zip, 8192, true))
                        using(var bson = new BsonWriter(buffer) { CloseOutput = false})
                        {
                            LogServer($"ready to write message {j}");
                            Signal.WaitOne();
                            LogServer($"writing message {j}");
                            serializer.Serialize(bson, Envelope.Create(MessageLength));
                            bson.Flush();
                            LogServer($"message written {j}");
                        }
                }
            }
            finally
            {
                listener.Stop();
            }
        }

        private static void RunClient()
        {
            var serializer = GetSerializer();
            using (var client = new TcpClient())
            {
                client.ConnectAsync("localhost", 27111).GetAwaiter().GetResult();
                client.ReceiveTimeout = 60000;
                using (var stream = client.GetStream())
                {
                    for (var j = 0; j < NumWrites; j++)
                    using (var zip = new DeflateStream(stream, CompressionMode.Decompress, true))
                    using (var buffer = new BufferedStream2(zip, 4096, true))
                    using (var bson = new BsonReader(buffer) { CloseInput = false })
                    {
                        LogClient($"reading message {j}");
                        var message = serializer.Deserialize<Envelope>(bson).Message;
                        LogClient($"message read {j}");
                        Signal.Set();
                    }
                }
            }
        }

#region totes-region-bro
        static readonly AutoResetEvent Signal = new AutoResetEvent(true);

        static void LogClient(string message)
        {
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(message);
        }

        static void LogServer(string message)
        {
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(message);
        }

        public class Envelope
        {
            public string Message { get; set; }

            public static Envelope Create(int length)
            {
                return new Envelope
                {
                    Message = CreateString(length)
                };
            }
        }

        static JsonSerializer GetSerializer()
        {
            var serializer = JsonSerializer.Create();
            serializer.Formatting = Formatting.None;
            serializer.ContractResolver = new DefaultContractResolver();
            serializer.TypeNameHandling = TypeNameHandling.Auto;
            serializer.TypeNameAssemblyFormat = FormatterAssemblyStyle.Simple;
            serializer.DateFormatHandling = DateFormatHandling.IsoDateFormat;
            return serializer;
        }

        static Random rd = new Random();
        internal static string CreateString(int stringLength)
        {
            const string allowedChars = "ABCDEFGHJKLMNOPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz0123456789!@$?_-";
            char[] chars = new char[stringLength];

            for (int i = 0; i < stringLength; i++)
            {
                chars[i] = allowedChars[rd.Next(0, allowedChars.Length)];
            }

            return new string(chars);
        }
#endregion
    }
}
