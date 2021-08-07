using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BlockingCollectionFun
{
    class Program
    {
        static async Task Main(string[] args)
        {
            for (var i = 0; i < 2; i++)
            {
                ConsoleWriter.WriteMessage($"====== starting run {i} ======");
                await ProduceAndImport(12, CancellationToken.None);
                ConsoleWriter.WriteMessage($"====== ended run {i} ======");
            }
            
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

        public static void WriteMessage(
            string message, 
            ConsoleColor? backgroundColor = null,
            ConsoleColor? foregroundColor = null)
        {
            message = $"[{DateTime.UtcNow:O}] {message}";
            
            lock (MessageLock)
            {
                if (backgroundColor != null)
                {
                    Console.BackgroundColor = (ConsoleColor) backgroundColor;
                }

                if (foregroundColor != null)
                {
                    Console.ForegroundColor = (ConsoleColor) foregroundColor;
                }
                
                Console.WriteLine(message);
                Console.ResetColor();
            }
        }
    }
}