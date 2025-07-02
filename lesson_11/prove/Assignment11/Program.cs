// This assignment meets with category 4: Meet Requirements
// I gave it this categroy because the program has workers and threads added and the program runs faster than the base program 
//origonally given.

namespace ConsoleApp1;

using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;

public class Assignment11
{
    private const long START_NUMBER = 10_000_000_000;
    private const int RANGE_COUNT = 1_000_000;
    private const int WORKER_COUNT = 10;

    private static bool IsPrime(long n)
    {
        if (n <= 3) return n > 1;
        if (n % 2 == 0 || n % 3 == 0) return false;

        for (long i = 5; i * i <= n; i = i + 6)
        {
            if (n % i == 0 || n % (i + 2) == 0)
                return false;
        }
        return true;
    }

    public static void Main(string[] args)
    {
        var queue = new ConcurrentQueue<long>();
        int numbersProcessed = 0;
        int primeCount = 0;
        var lockObject = new object();

        Console.WriteLine("Prime numbers found:");

        var stopwatch = Stopwatch.StartNew();

        Thread[] workers = new Thread[WORKER_COUNT];
        for (int i = 0; i < WORKER_COUNT; i++)
        {
            workers[i] = new Thread(() =>
            {
                while (true)
                {
                    if (!queue.TryDequeue(out long number))
                    {
                        if (Interlocked.Increment(ref numbersProcessed) > RANGE_COUNT)
                            return;
                        continue;
                    }

                    if (IsPrime(number))
                    {
                        Interlocked.Increment(ref primeCount);
                        lock (lockObject)
                        {
                            Console.Write($"{number}, ");
                        }
                    }

                    Interlocked.Increment(ref numbersProcessed);
                }
            });
            workers[i].Start();
        }

        for (long i = START_NUMBER; i < START_NUMBER + RANGE_COUNT; i++)
        {
            queue.Enqueue(i);
        }

        foreach (var worker in workers)
        {
            worker.Join();
        }

        stopwatch.Stop();

        Console.WriteLine();
        Console.WriteLine();
        Console.WriteLine($"Numbers processed = {numbersProcessed}");
        Console.WriteLine($"Primes found      = {primeCount}");
        Console.WriteLine($"Total time        = {stopwatch.Elapsed}");
    }
}