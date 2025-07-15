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
    
    public static async Task<bool> DepthFS(long familyId, Tree tree)
    {
        if (familyId == 0) return true;

        var family = await FetchFamilyAsync(familyId);
        if (family == null) return false;

        tree.AddFamily(family);

        var tasks = new List<Task<Person?>>();
        if (family.HusbandId != 0) tasks.Add(FetchPersonAsync(family.HusbandId));
        if (family.WifeId != 0) tasks.Add(FetchPersonAsync(family.WifeId));
        foreach (var childId in family.Children) if (childId != 0) tasks.Add(FetchPersonAsync(childId));

        var people = await Task.WhenAll(tasks);
        foreach (var person in people) if (person != null) tree.AddPerson(person);

        var parentTasks = new List<Task<bool>>();
        var husband = people.FirstOrDefault(p => p?.Id == family.HusbandId);
        var wife = people.FirstOrDefault(p => p?.Id == family.WifeId);

        if (husband != null && husband.ParentId != 0)
            parentTasks.Add(Task.Run(() => DepthFS(husband.ParentId, tree)));
        if (wife != null && wife.ParentId != 0)
            parentTasks.Add(Task.Run(() => DepthFS(wife.ParentId, tree)));

        var results = await Task.WhenAll(parentTasks);
        return results.All(r => r);
    }

    public static async Task<bool> BreathFS(long familyId, Tree tree)
    {
        if (familyId == 0) return true;

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
                if (family.HusbandId != 0) tasks.Add(FetchPersonAsync(family.HusbandId));
                if (family.WifeId != 0) tasks.Add(FetchPersonAsync(family.WifeId));
                foreach (var childId in family.Children) if (childId != 0) tasks.Add(FetchPersonAsync(childId));

                var people = await Task.WhenAll(tasks);
                foreach (var person in people) if (person != null) tree.AddPerson(person);

                var husband = people.FirstOrDefault(p => p?.Id == family.HusbandId);
                var wife = people.FirstOrDefault(p => p?.Id == family.WifeId);

                if (husband != null && husband.ParentId != 0) queue.Enqueue(husband.ParentId);
                if (wife != null && wife.ParentId != 0) queue.Enqueue(wife.ParentId);
            }
        }

        const int maxConcurrentTasks = 10;
        var workerTasks = new List<Task>();
        for (int i = 0; i < maxConcurrentTasks; i++)
            workerTasks.Add(Task.Run(ProcessFamilyAsync));

        await Task.WhenAll(workerTasks);
        return true;
    }
}