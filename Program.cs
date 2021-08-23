using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CosmosEvents
{
    class Program
    {
        private static Client _client = null;

        static void Main(string[] args)
        {
            MainAsync(args).GetAwaiter().GetResult();
            Console.ReadLine();
        }

        public static async Task MainAsync(string[] args)
        {
            //await Bulk();
            //await SingleItem();
            Console.WriteLine("Choose an option:");
            Console.WriteLine("1. Load sports events documents");
            Console.WriteLine("2. Run queries");
           
            _client = new Client();

            ConsoleKeyInfo key = Console.ReadKey();

            Console.WriteLine("");
            switch (key.Key)
            {
                case ConsoleKey.D1:
                    {
                        await _client.Initialize();

                        await Bulk(Utils.GenerateMarathon("Marathon New York", DateTime.Parse("2021-6-6"), "Olympic", 30000));
                        await Bulk(Utils.GenerateMarathon("Marathon Amsterdam", DateTime.Parse("2021-8-1"), "Olympic", 15000));
                        await Bulk(Utils.GenerateMarathon("Marathon Madrid", DateTime.Parse("2021-10-1"), "Trial", 5000));

                        await Bulk(Utils.Generate("Triathlon Finland", DateTime.Parse("2021-2-1"), "Olympic", 10000));
                        await Bulk(Utils.Generate("Triathlon Canada", DateTime.Parse("2021-3-1"), "Olympic", 15000));
                        await Bulk(Utils.Generate("Triathlon Argentina", DateTime.Parse("2021-5-1"), "Trial", 15000));
                        await Bulk(Utils.Generate("Triathlon Amsterdam", DateTime.Parse("2021-9-1"), "Trial", 10000));

                        Console.WriteLine("");

                        break;

                    };
                case ConsoleKey.D2:
                    {
                        await Query("Marathon New York", DateTime.Parse("2021-6-6"), 51);
                        break;
                    }
            }

        }

        public static async Task SingleItem()
        {
            Triathlon t = Utils.Generate("Triathlon Asia", DateTime.Parse("2021-5-19"), "Trial", 1)[0];

            await _client.AddItem(t, t.Eventname);
        }

        public static async Task Query(string eventName, DateTime eventDate, int participantId)
        {

            Console.WriteLine("Initial queries");

           
            await _client.Old_Q1TopRanked(eventName, eventDate);
            await _client.Q2ViewAllEventsInYear(participantId);
            await _client.Old_Q3ViewParticipantsPerEvent(eventName);
            Marathon[] items = await _client.Old_Q4ViewHighScoreForParticipant(eventName, participantId);

          
            Console.WriteLine("Press any key run the optimized queries...");
            Console.ReadLine();

            Console.WriteLine("Optimized queries");

            await _client.New_Q1TopRanked(eventName, eventDate);
            await _client.Q2ViewAllEventsInYear(participantId);
            await _client.New_Q3ViewParticipantsPerEvent(eventName);

            if (items.Length > 0)
                await _client.New_Q4ViewHighScoreForParticipant("Marathon New York", items[0].Id);

        }

        public static async Task Bulk(dynamic[] items)
        {
            Console.Write("Inserting {0} items...", items.Count<dynamic>());
            await _client.BulkInsert(items);

            Console.WriteLine("done");          

        }

        private static Triathlon[] GenerateTriathlons(string eventName, int numberOfItems, DateTime eventDate, string category)
        {
            return Utils.Generate(eventName, eventDate, category, numberOfItems);
        }

        private static Marathon[] Marathon(string eventName, int numberOfItems, DateTime eventDate, string category)
        {
            return Utils.GenerateMarathon(eventName, eventDate, category, numberOfItems);
        }

    }
}
