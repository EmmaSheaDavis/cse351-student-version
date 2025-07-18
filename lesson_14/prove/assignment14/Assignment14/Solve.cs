// I achieved a 3 on this assignment becasue even though I have the DepthFS running below 10 seconds 
//I was never able to get the BreathFS to run below 16 seconds.

using System.Collections.Concurrent;
using Newtonsoft.Json.Linq;

namespace Assignment14;

public static class Solve
{
    private static readonly HttpClient HttpClient = new()
    {
        Timeout = TimeSpan.FromSeconds(180)
    };
    public const string TopApiUrl = "http://127.0.0.1:8123";
    public static async Task<JObject?> GetDataFromServerAsync(string url)
    {
        try
        {
            var jsonString = await HttpClient.GetStringAsync(url);
            return JObject.Parse(jsonString);
        }
        catch (HttpRequestException e)
        {
            Console.WriteLine($"Error fetching data from {url}: {e.Message}");
            return null;
        }
    }

    private static async Task<Person?> FetchPersonAsync(long personId)
    {
        var personJson = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/person/{personId}");
        return personJson != null ? Person.FromJson(personJson.ToString()) : null;
    }

    private static async Task<Family?> FetchFamilyAsync(long familyId)
    {
        var familyJson = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/family/{familyId}");
        return familyJson != null ? Family.FromJson(familyJson.ToString()) : null;
    }
    
    public static async Task<bool> DepthFS(long familyId, Tree tree, HashSet<long> processedPersons = null)
    {
        if (familyId == 0) return true;

        processedPersons ??= new HashSet<long>();

        var family = await FetchFamilyAsync(familyId);
        if (family == null) return false;

        tree.AddFamily(family);

        var tasks = new List<Task<Person?>>();
        var personIds = new List<long>();
        if (family.HusbandId != 0 && !processedPersons.Contains(family.HusbandId)) personIds.Add(family.HusbandId);
        if (family.WifeId != 0 && !processedPersons.Contains(family.WifeId)) personIds.Add(family.WifeId);
        foreach (var childId in family.Children) if (childId != 0 && !processedPersons.Contains(childId)) personIds.Add(childId);

        foreach (var id in personIds) tasks.Add(FetchPersonAsync(id));
        var people = await Task.WhenAll(tasks);
        foreach (var person in people) if (person != null) { processedPersons.Add(person.Id); tree.AddPerson(person); }

        var parentTasks = new List<Task<bool>>();
        var husband = people.FirstOrDefault(p => p?.Id == family.HusbandId);
        var wife = people.FirstOrDefault(p => p?.Id == family.WifeId);

        if (husband != null && husband.ParentId != 0 && !processedPersons.Contains(husband.ParentId))
            parentTasks.Add(Task.Run(() => DepthFS(husband.ParentId, tree, processedPersons)));
        if (wife != null && wife.ParentId != 0 && !processedPersons.Contains(wife.ParentId))
            parentTasks.Add(Task.Run(() => DepthFS(wife.ParentId, tree, processedPersons)));

        var results = await Task.WhenAll(parentTasks);
        return results.All(r => r);
    }

    public static async Task<bool> BreathFS(long familyId, Tree tree, HashSet<long> processedPersons = null)
    {
        if (familyId == 0) return true;

        processedPersons ??= new HashSet<long>();

        var queue = new ConcurrentQueue<long>();
        queue.Enqueue(familyId);
        var processedFamilies = new ConcurrentBag<long>();

        async Task ProcessFamilyAsync()
        {
            while (queue.TryDequeue(out var currentFamilyId))
            {
                if (processedFamilies.Contains(currentFamilyId)) continue;
                processedFamilies.Add(currentFamilyId);

                var family = await FetchFamilyAsync(currentFamilyId);
                if (family == null) continue;

                tree.AddFamily(family);

                var tasks = new List<Task<Person?>>();
                var personIds = new List<long>();
                if (family.HusbandId != 0 && !processedPersons.Contains(family.HusbandId)) personIds.Add(family.HusbandId);
                if (family.WifeId != 0 && !processedPersons.Contains(family.WifeId)) personIds.Add(family.WifeId);
                foreach (var childId in family.Children) if (childId != 0 && !processedPersons.Contains(childId)) personIds.Add(childId);

                foreach (var id in personIds) tasks.Add(FetchPersonAsync(id));
                var people = await Task.WhenAll(tasks);
                foreach (var person in people) if (person != null) { processedPersons.Add(person.Id); tree.AddPerson(person); }

                var husband = people.FirstOrDefault(p => p?.Id == family.HusbandId);
                var wife = people.FirstOrDefault(p => p?.Id == family.WifeId);

                if (husband != null && husband.ParentId != 0 && !processedPersons.Contains(husband.ParentId)) queue.Enqueue(husband.ParentId);
                if (wife != null && wife.ParentId != 0 && !processedPersons.Contains(wife.ParentId)) queue.Enqueue(wife.ParentId);
                foreach (var childId in family.Children) if (childId != 0 && !processedPersons.Contains(childId) && people.Any(p => p?.Id == childId))
                    queue.Enqueue((await FetchPersonAsync(childId))?.ParentId ?? 0);
            }
        }

        const int maxConcurrentTasks = 30;
        var workerTasks = new List<Task>();
        for (int i = 0; i < maxConcurrentTasks; i++)
            workerTasks.Add(Task.Run(ProcessFamilyAsync));

        await Task.WhenAll(workerTasks);
        return true;
    }
}