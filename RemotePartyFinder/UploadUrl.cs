using System;
using Newtonsoft.Json;

namespace RemotePartyFinder;

public record UploadUrl(string Url)
{
    public string Url { get; set; } = Url;
    public bool IsDefault { get; init; }
    public bool IsEnabled { get; set; } = true;
    
    [JsonIgnore]
    public int FailureCount { get; set; }
    
    [JsonIgnore]
    public DateTime LastFailureTime { get; set; }
}
