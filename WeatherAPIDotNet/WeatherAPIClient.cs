//Copyright (c) Microsoft Corporation

using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;


namespace Microsoft.Azure.Batch.Samples.TopNWordsSample
{
    /// <summary>
    /// This class has the code for each task. The task reads the
    /// blob assigned to it and determine TopNWords and writes
    /// them to standard out
    /// </summary>
    public class WeatherAPIClient
    {
        public static async Task TaskMain(string[] args)
        {
            long num = Int64.Parse(args[1]);
            string nodeId = args[2];
            using (var httpClient = new HttpClient())
            {
                var apiUrl = String.Format("http://api.openweathermap.org/data/2.5/weather?id={0}&appid=b4db3a51a5c883e2244c5a4e977e3ec9", num);
                using (var response = httpClient.GetAsync(apiUrl).Result)
                {

                    string apiResponse = await response.Content.ReadAsStringAsync();
                    Console.WriteLine(apiResponse.ToString());
                    var weatherData = WeatherData.FromJson(apiResponse);
                    string timenow = DateTime.Now.ToString("h:mm:ss tt");
                    Console.WriteLine(apiResponse.ToString());
                    weatherData.CurrentTime = timenow;
                    weatherData.NodeId = nodeId;
                    WeatherAPIContext context = new WeatherAPIContext();
                    Console.WriteLine("before add weather data");
                    context.WeatherData.Add(weatherData);
                    Console.WriteLine("after add weather data");
                    context.SaveChanges();
                    Console.WriteLine("after add save changes");
                }
            }
        }
        

    }
    public partial class WeatherData
    {
        [JsonProperty("coord")]
        public Coord Coord { get; set; }

        [JsonProperty("weather")]
        public List<Weather> Weather { get; set; }

        [JsonProperty("base")]
        public string Base { get; set; }

        [JsonProperty("main")]
        public Main Main { get; set; }

        [JsonProperty("visibility")]
        public long Visibility { get; set; }

        [JsonProperty("wind")]
        public Wind Wind { get; set; }

        [JsonProperty("clouds")]
        public Clouds Clouds { get; set; }

        [JsonProperty("dt")]
        public long Dt { get; set; }

        [JsonProperty("sys")]
        public Sys Sys { get; set; }

        [JsonProperty("timezone")]
        public long Timezone { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("name")]
        public string Name { get; set; }

        [JsonProperty("cod")]
        public long Cod { get; set; }
        public string CurrentTime { get; set; }
        public string NodeId { get; set; }
    }

    [Owned]
    public partial class Clouds
    {
        [JsonProperty("all")]
        public long All { get; set; }
    }

    [Owned]
    public partial class Coord
    {
        [JsonProperty("lon")]
        public double Lon { get; set; }

        [JsonProperty("lat")]
        public double Lat { get; set; }
    }
    [Owned]
    public partial class Main
    {
        [JsonProperty("temp")]
        public double Temp { get; set; }

        [JsonProperty("feels_like")]
        public double FeelsLike { get; set; }

        [JsonProperty("temp_min")]
        public double TempMin { get; set; }

        [JsonProperty("temp_max")]
        public double TempMax { get; set; }

        [JsonProperty("pressure")]
        public long Pressure { get; set; }

        [JsonProperty("humidity")]
        public long Humidity { get; set; }
    }
    [Owned]
    public partial class Sys
    {
        [JsonProperty("type")]
        public long Type { get; set; }

        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("country")]
        public string Country { get; set; }

        [JsonProperty("sunrise")]
        public long Sunrise { get; set; }

        [JsonProperty("sunset")]
        public long Sunset { get; set; }
    }
    [Owned]
    public partial class Weather
    {
        [JsonProperty("id")]
        public string Id { get; set; }

        [JsonProperty("main")]
        public string Main { get; set; }

        [JsonProperty("description")]
        public string Description { get; set; }

        [JsonProperty("icon")]
        public string Icon { get; set; }
    }
    [Owned]
    public partial class Wind
    {
        [JsonProperty("speed")]
        public double Speed { get; set; }

        [JsonProperty("deg")]
        public long Deg { get; set; }
    }

    public partial class WeatherData
    {
        public static WeatherData FromJson(string json) => JsonConvert.DeserializeObject<WeatherData>(json, Converter.Settings);
    }

    public static class Serialize
    {
        public static string ToJson(this WeatherData self) => JsonConvert.SerializeObject(self, Converter.Settings);
    }

    internal static class Converter
    {
        public static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            MetadataPropertyHandling = MetadataPropertyHandling.Ignore,
            DateParseHandling = DateParseHandling.None,
            Converters =
            {
                new IsoDateTimeConverter { DateTimeStyles = DateTimeStyles.AssumeUniversal }
            },
        };
    }
    public class WeatherAPIContext : DbContext
    {

        public DbSet<WeatherData> WeatherData { get; set; }


        protected override void OnConfiguring(DbContextOptionsBuilder optionsBuilder)
        {
            optionsBuilder.UseSqlServer("Server=tcp:weather-api-results.database.windows.net,1433;Initial Catalog=WeatherAPIResults;Persist Security Info=False;User ID=weather-api-results;Password=quora456@;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;");

        }
        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<WeatherData>(en =>
            {
                en.OwnsOne(e => e.Clouds);
                en.OwnsOne(e => e.Coord);
                en.OwnsOne(e => e.Wind);
                en.OwnsOne(e => e.Main);
                en.OwnsOne(e => e.Sys);
                en.OwnsMany(e => e.Weather);
            });
        }

    }


}
