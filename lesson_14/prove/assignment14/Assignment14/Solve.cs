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

    // This function retrieves JSON from the server
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

    // This function takes in a person ID and retrieves a Person object
    // Hint: It can be used in a "new List<Task<Person?>>()" list
    private static async Task<Person?> FetchPersonAsync(long personId)
    {
        var personJson = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/person/{personId}");
        return personJson != null ? Person.FromJson(personJson.ToString()) : null;
    }

    // This function takes in a family ID and retrieves a Family object
    // Hint: It can be used in a "new List<Task<Family?>>()" list
    private static async Task<Family?> FetchFamilyAsync(long familyId)
    {
        var familyJson = await Solve.GetDataFromServerAsync($"{Solve.TopApiUrl}/family/{familyId}");
        return familyJson != null ? Family.FromJson(familyJson.ToString()) : null;
    }
    
    // =======================================================================================================
    public static async Task<bool> DepthFS(long familyId, Tree tree)
    {
        // Note: invalid IDs are zero not null

        // TODO - add you solution here
        if (familyId == 0) return true; // Invalid family ID

        // Fetch family data
        var family = await FetchFamilyAsync(familyId);
        if (family == null) return false;

        // Add family to tree (thread-safe as Tree.AddFamily checks for duplicates)
        tree.AddFamily(family);

        // Prepare tasks for fetching husband, wife, and children concurrently
        var tasks = new List<Task<Person?>>();
        if (family.HusbandId != 0)
            tasks.Add(FetchPersonAsync(family.HusbandId));
        if (family.WifeId != 0)
            tasks.Add(FetchPersonAsync(family.WifeId));
        foreach (var childId in family.Children)
            if (childId != 0)
                tasks.Add(FetchPersonAsync(childId));

        // Await all person fetches
        var people = await Task.WhenAll(tasks);

        // Add valid people to tree
        foreach (var person in people)
            if (person != null)
                tree.AddPerson(person);

        // Recursively process parents' families concurrently
        var parentTasks = new List<Task<bool>>();
        var husband = people.FirstOrDefault(p => p?.Id == family.HusbandId);
        var wife = people.FirstOrDefault(p => p?.Id == family.WifeId);

        if (husband != null && husband.ParentId != 0)
            parentTasks.Add(Task.Run(() => DepthFS(husband.ParentId, tree)));
        if (wife != null && wife.ParentId != 0)
            parentTasks.Add(Task.Run(() => DepthFS(wife.ParentId, tree)));

        // Await parent family processing
        var results = await Task.WhenAll(parentTasks);
        return results.All(r => r);
        return true;
    }

    // =======================================================================================================
    public static async Task<bool> BreathFS(long famid, Tree tree)
    {
        // Note: invalid IDs are zero not null
        // TODO - add you solution here
        return true;
    }
}
