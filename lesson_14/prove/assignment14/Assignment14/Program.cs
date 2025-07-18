/*
 * Course    : CSE 351
 * Assignment: 14
 */
using System;
using Assignment14;
using Newtonsoft.Json.Linq;

class Program
{
    public static async Task run_part(long startId, int generations, string title, Func<long, Tree, HashSet<long>, Task<bool>> func)
    {
        var startData = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/start/{generations}");
        Console.WriteLine(startData?.ToString() ?? "null");

        Logger.Write("\n");
        Logger.Write("".PadRight(45, '#'));
        Logger.Write(title + ": " + generations + " generations");
        Logger.Write("".PadRight(45, '#'));
        
        var timer = System.Diagnostics.Stopwatch.StartNew();
        var tree = new Tree(startId);
        await func(startId, tree, new HashSet<long>());
        timer.Stop();
        double totalTime = timer.Elapsed.TotalSeconds;

        var serverData = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/end");
        Console.WriteLine(serverData?.ToString() ?? "null");

        Logger.Write("");
        Logger.Write($"total_time                  : {totalTime:F5}");
        Logger.Write($"Generations                 : {generations}");

        Logger.Write("STATS        Retrieved | Server details");
        Logger.Write($"People  : {tree.PersonCount,12:N0} | {(serverData?["people"]?.Value<long>() ?? 0),14:N0}");
        Logger.Write($"Families: {tree.FamilyCount,12:N0} | {(serverData?["families"]?.Value<long>() ?? 0),14:N0}");
        Logger.Write($"API Calls                   : {(serverData?["api"]?.Value<long>() ?? 0)}");
        Logger.Write($"Max number of threads       : {(serverData?["threads"]?.Value<long>() ?? 0)}");
    }
    
    static async Task Main()
    {        
        Logger.Configure(minimumLevel: LogLevel.Debug, logToFile: true, filePath: "../../../assignment.log");
        
        var data = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}");
        long start_id = data?["start_family_id"]?.Value<long>() ?? 0; // Handle null case
        
        try
        {
            foreach (string line in File.ReadLines("runs.txt"))
            {
                string[] parts = line.Split(',');

                if (parts.Length >= 2 && 
                    int.TryParse(parts[0], out int partToRun) && 
                    int.TryParse(parts[1], out int generations))
                {
                    switch (partToRun)
                    {
                        case 1:
                            await run_part(start_id, generations, "Depth First Search", Solve.DepthFS);
                            break;
                        case 2:
                            await run_part(start_id, generations, "Breath First Search", Solve.BreathFS);
                            break;
                    }
                }
                else
                {
                    Console.WriteLine($"Skipping invalid line: {line}");
                }
            }
        }
        catch (FileNotFoundException)
        {
            Console.Error.WriteLine("Error: runs.txt not found.");
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"An unexpected error occurred: {ex.Message}");
        }        
    }
}