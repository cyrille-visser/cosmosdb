using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosEvents
{
    class Triathlon
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        public string Eventname { get; set; }
        public DateTime Eventdate { get; set; }
        public int ParticipantId { get; set; }
        public string ParticipantLastname { get; set; }
        public string ParticipantFirstname { get; set; }
        public string Category { get; set; }
        public TimeSpan SwimmingScore { get; set; }
        public TimeSpan CyclingScore { get; set; }
        public TimeSpan RunningScore { get; set; }
        public TimeSpan TotalScore { get; set; }

        // The ToString() method is used to format the output, it's used for demo purpose only. It's not required by Azure Cosmos DB
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }

    class Marathon
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }
        public string Eventname { get; set; }
        public DateTime Eventdate { get; set; }
        public int ParticipantId { get; set; }
        public string ParticipantLastname { get; set; }
        public string ParticipantFirstname { get; set; }
        public string Category { get; set; }
        public TimeSpan Score { get; set; }

        // The ToString() method is used to format the output, it's used for demo purpose only. It's not required by Azure Cosmos DB
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
