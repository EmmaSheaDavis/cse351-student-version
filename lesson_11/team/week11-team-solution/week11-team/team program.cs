namespace week11_team;

/*
 * Course: CSE 3541
 * Week  : 11
 *
 * - You need to run the server in Python for this program first
 * 
 * Goal: (Look for to do comments)
 * 1) install package Newtonsoft.Json.Linq for this project.
 *    - very easy to do in Rider (right click on the error)
 *    - for VSCode, do search for instructions (ie., "how to install Newtonsoft.Json.Linq in dotnet")
 * 2) Make the program faster
 * 3) Add an int to count the number of calls to the server.
 *
 * Hint
 * - look at the documentation of the method Select() for lists, also Task.WhenAll()
 * - https://www.dotnetperls.com/select
 * - https://www.csharptutorial.net/csharp-linq/linq-select/
 */

using System;
using System.Diagnostics;
using System.Net.Http;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using Newtonsoft.Json.Linq;

class Program
{
    private static readonly HttpClient HttpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(5) }; // Set timeout
    private const string TopApiUrl = "http://127.0.0.1:8790";
    private static int callCount = 0;
    private static readonly SemaphoreSlim Semaphore = new SemaphoreSlim(15); // Limit to 10 concurrent requests
    
    private static JObject? GetDataFromServer(string url)
    {
        try
        {
            Semaphore.Wait();
            Interlocked.Increment(ref callCount);
            var jsonString = HttpClient.GetStringAsync(url).GetAwaiter().GetResult();
            return JObject.Parse(jsonString);
        }
        catch (HttpRequestException e)
        {
            lock (Console.Out)
            {
                Console.WriteLine($"Error fetching data from {url}: {e.Message}");
            }
            return null;
        }
        catch (TaskCanceledException)
        {
            lock (Console.Out)
            {
                Console.WriteLine($"Timeout fetching data from {url}");
            }
            return null;
        }
        finally
        {
            Semaphore.Release();
        }
    }    
    
    private static void GetUrls(JObject filmData, string kind)
    {
        var urls = filmData[kind]?.ToObject<List<string>>();
        if (urls == null || !urls.Any())
            return;

        lock (Console.Out)
        {
            Console.WriteLine(kind.ToUpper());
            Console.WriteLine($"  Number of urls = {urls.Count}");
        }
        
        var threads = new List<Thread>();
        foreach (var url in urls)
        {
            var thread = new Thread(() =>
            {
                var item = GetDataFromServer(url);
                if (item != null)
                {
                    var name = item["name"] ?? item["title"];
                    lock (Console.Out)
                    {
                        Console.WriteLine($"  - {name}");
                    }
                }
            });
            threads.Add(thread);
            thread.Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }
    }    
    
    static void Main() // Changed to non-async
    {
        var stopwatch = Stopwatch.StartNew();
        
        var film6 = GetDataFromServer($"{TopApiUrl}/films/6");
        if (film6 == null)
        {
            lock (Console.Out)
            {
                Console.WriteLine("Failed to fetch film data. Exiting.");
            }
            return;
        }

        lock (Console.Out)
        {
            Console.WriteLine(film6["director"]);
        }

        var categories = new[] { "characters", "planets", "starships", "vehicles", "species" };
        var threads = new List<Thread>();

        foreach (var kind in categories)
        {
            var thread = new Thread(() => GetUrls(film6, kind));
            threads.Add(thread);
            thread.Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }
        
        stopwatch.Stop();
        
        lock (Console.Out)
        {
            Console.WriteLine($"Total calls to the server = {callCount}");
            Console.WriteLine($"Total execution time: {stopwatch.Elapsed.TotalSeconds:F2} seconds");
        }
    }
}