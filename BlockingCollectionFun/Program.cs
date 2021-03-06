using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BlockingCollectionFun
{
    class Program
    {
        private static readonly ConsoleWriter ConsoleWriter = new ConsoleWriter();
        static async Task Main(string[] args)
        {
            for (var i = 0; i < 2; i++)
            {
                ConsoleWriter.WriteMessage($"====== starting run {i} ======");
                await ProduceAndImport(12, CancellationToken.None);
                ConsoleWriter.WriteMessage($"====== ended run {i} ======");
            }

            await ConsoleWriter.WaitUntilEmpty();
        }

        private static Task ProduceAndImport(int maxDegreesOfParallelism, CancellationToken ct)
        {
            var bc = new BlockingCollection<string>(maxDegreesOfParallelism * 2);
            var p1 = Task.Run(() => Produce("P1", bc, ct), ct);
            var p2 = Task.Run(() => Produce("P2", bc, ct), ct);
            var c = Task.Run(() => Consume(maxDegreesOfParallelism, bc, ct), ct);
            Task.WaitAll(p1, p2);
            ConsoleWriter.WriteMessage("PRODUCING IS COMPLETED", ConsoleColor.Yellow, ConsoleColor.Black);
            bc.CompleteAdding();
            c.Wait(ct);
            return Task.CompletedTask;
        }

        private static async Task Consume(int maxDegreesOfParallelism, BlockingCollection<string> bc, CancellationToken ct)
        {
            ConsoleWriter.WriteMessage("CONSUMPTION HAS STARTED", ConsoleColor.White, ConsoleColor.Green);
            var ss = new SemaphoreSlim(maxDegreesOfParallelism);
            var tasks = new ConcurrentDictionary<string, Task>();
            foreach (var item in bc.GetConsumingEnumerable())
            {
                await ss.WaitAsync(ct);

                tasks.TryAdd(item, Task.Run(() =>
                {
                    try
                    { 
                        DoWork(item, ct);
                    }
                    finally
                    {
                        ss.Release();
                    }
                    
                }, ct));
                ConsoleWriter.WriteMessage($"There are approximately {ss.CurrentCount} / {maxDegreesOfParallelism} " +
                                           $"available in the SemaphoreSlim and {bc.Count} in the blocking collection");
            }

            await Task.Run(() =>
            {
                while (tasks.Any(a => !a.Value.IsCompleted))
                {
                    foreach (var completed in tasks.Where(w => w.Value.IsCompleted))
                    {
                        tasks.TryRemove(completed.Key, out _);
                    }

                    Task.Delay(100, ct).Wait(ct);
                }
                
                ss.Dispose();
            }, ct);
            
            ConsoleWriter.WriteMessage("The consumption has ended!", ConsoleColor.Green, ConsoleColor.White);
        }

        private static Task DoWork(string work, CancellationToken ct)
        {
            if (!ct.IsCancellationRequested)
            {
                ConsoleWriter.WriteMessage($"Starting Work: {work}");
                var id = int.Parse(work.Split(':').Last().Trim());
                Task.Delay(100 * id, ct).Wait(ct);
                ConsoleWriter.WriteMessage($"Completed Work: {work}");
            }

            return Task.CompletedTask;
        }

        static async Task Produce(string name, BlockingCollection<string> bc, CancellationToken ct)
        {
            for (var i = 0; i < 100; i++)
            {
                if (ct.IsCancellationRequested)
                {
                    return;
                }

                var alerted = false;
                while (!bc.TryAdd($"{name}: {i}"))
                {
                    if (!alerted)
                    {
                        ConsoleWriter.WriteMessage($"{name} was unable to enqueue {i} " +
                                                   $"and there are roughly {bc.Count} in the blocking collection",
                            ConsoleColor.Red, ConsoleColor.Black);
                        alerted = true;
                    }
                    await Task.Delay(100, ct);
                }
                
                ConsoleWriter.WriteMessage($"{name} has just enqueued {i} " +
                                           $"and there are roughly {bc.Count} in the blocking collection",
                    ConsoleColor.Blue, ConsoleColor.White);

                await Task.Delay(10 * i, ct);
            }
            
            ConsoleWriter.WriteMessage($"{name} HAS COMPLETED PRODUCING");
        }
    }
    
    public class ConsoleWriter
    {
        private static readonly object MessageLock = new object();
        private static readonly ConcurrentQueue<QueuedMessage> Messages = new ConcurrentQueue<QueuedMessage>();
        private readonly Timer _messageTimer;

        public ConsoleWriter()
        {
            _messageTimer = new Timer(WriteMessages, null, TimeSpan.FromMilliseconds(100), TimeSpan.Zero);
        }

        private void WriteMessages(object state)
        {
            while (Messages.TryDequeue(out var msg))
            {
                WriteMessage(msg);
            }

            _messageTimer.Change(100, 0);
        }

        private class QueuedMessage
        {
            public string Message { get; set; }
            public ConsoleColor? BackgroundColor { get; set; }
            public ConsoleColor? ForegroundColor { get; set; }
        }

        public void WriteMessage(string message,
            ConsoleColor? backgroundColor = null,
            ConsoleColor? foregroundColor = null)
        {
            message = $"[{DateTime.UtcNow:O}] {message}";
            Messages.Enqueue(new QueuedMessage
            {
                Message = message,
                BackgroundColor = backgroundColor,
                ForegroundColor = foregroundColor
            });
        }
        
        private void WriteMessage(QueuedMessage msg)
        {
            lock (MessageLock)
            {
                if (msg.BackgroundColor != null)
                {
                    Console.BackgroundColor = (ConsoleColor) msg.BackgroundColor;
                }

                if (msg.ForegroundColor != null)
                {
                    Console.ForegroundColor = (ConsoleColor) msg.ForegroundColor;
                }
                
                Console.WriteLine(msg.Message);
                Console.ResetColor();
            }
        }

        public static async Task WaitUntilEmpty()
        {
            while (!Messages.IsEmpty)
            {
                await Task.Delay(100);
            }
        }
    }
}