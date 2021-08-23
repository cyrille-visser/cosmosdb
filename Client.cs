using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace CosmosEvents
{
    class Client
    {
        private static CosmosClient _client;
        private Container _container;

        public Client()
        {

            string conString = ConfigurationManager.AppSettings["ConnectionString"];

            _client = new CosmosClient(conString, new CosmosClientOptions() { AllowBulkExecution = true }) ;
        }
        
        /// <summary>
        /// Create database and container if not exists.
        /// </summary>
        /// <returns></returns>
        public async Task Initialize()
        {
            try
            {
                string databaseId = ConfigurationManager.AppSettings["Database"];
                string containerId = ConfigurationManager.AppSettings["Container"];
                string partitionKeyPath = ConfigurationManager.AppSettings["PartitionKeyPath"];

                DatabaseResponse db = await _client.CreateDatabaseIfNotExistsAsync(databaseId);

                _container = await db.Database.CreateContainerIfNotExistsAsync(new ContainerProperties() { Id = containerId, PartitionKeyPath = partitionKeyPath }, ThroughputProperties.CreateAutoscaleThroughput(10000));
                
                await SetIndexPolicy();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
        }
        /// <summary>
        /// Add a single item to the container
        /// </summary>
        /// <param name="item"></param>
        /// <param name="partitionKeyValue"></param>
        /// <returns></returns>
        public async Task AddItem(dynamic item, string partitionKeyValue)
        {
            try
            {
                if (_container == null)
                    await this.Initialize();
                
                ItemResponse<dynamic> response = await _container.CreateItemAsync<dynamic>(item, new PartitionKey(partitionKeyValue));

                PrintStats(response);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.Conflict)
            {
                Console.WriteLine("Item in database already exists\n");
            }
            catch (Exception e)
            {
                Console.WriteLine("Error inserting item {0}", e.Message);
            }
        }

        /// <summary>
        /// Insert items in bulk. In case of throttling please decrease the number of documents or increase RU's.
        /// </summary>
        /// <param name="items"></param>
        /// <returns></returns>
        public async Task BulkInsert(dynamic[] items)
        {
            try
            {
                if (_container == null)
                    await this.Initialize();

                List<Task> concurrentTasks = new List<Task>();
                foreach (dynamic item in items)
                {
                    concurrentTasks.Add(_container.CreateItemAsync(item, new PartitionKey(item.Eventname)));
                }

                await Task.WhenAll(concurrentTasks);
            }
            catch (CosmosException e)
            {

                Console.WriteLine("Error {0}", e.Message);
            }

        }
        /// <summary>
        /// Query 1 not optimized.
        /// </summary>
        /// <param name="eventName"></param>
        /// <param name="eventDate"></param>
        /// <returns></returns>
        public async Task<dynamic[]> Old_Q1TopRanked(string eventName, DateTime eventDate)
        {
            if (_container == null)
                await this.Initialize();

            List<dynamic> list = new List<dynamic>();

            QueryDefinition query = new QueryDefinition("SELECT c.TotalScore FROM c WHERE c.Eventname = @Eventname AND c.Eventdate = @Eventdate ORDER BY c.TotalScore ASC")
                .WithParameter("@Eventname", eventName)
                .WithParameter("@Eventdate", eventDate);

            using (FeedIterator<dynamic> resultset = _container.GetItemQueryIterator<dynamic>(query))
            {
                while (resultset.HasMoreResults)
                {
                    FeedResponse<dynamic> response = await resultset.ReadNextAsync();
                    Console.WriteLine("Q1 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                    foreach (var item in response)
                    {
                        list.Add(item);
                    }
                }
            }

            return list.ToArray();
        }

        /// <summary>
        /// Query 1 optimized by adding TOP to the query.
        /// </summary>
        /// <param name="eventName"></param>
        /// <param name="eventDate"></param>
        /// <returns></returns>
        public async Task<dynamic[]> New_Q1TopRanked(string eventName, DateTime eventDate)
        {
            if (_container == null)
                await this.Initialize();

            List<dynamic> list = new List<dynamic>();

            QueryDefinition query = new QueryDefinition("SELECT TOP 100 c.TotalScore FROM c WHERE c.Eventname = @Eventname AND c.Eventdate = @Eventdate ORDER BY c.TotalScore ASC")
                .WithParameter("@Eventname", eventName)
                .WithParameter("@Eventdate", eventDate);
         
            using (FeedIterator<dynamic> resultset = _container.GetItemQueryIterator<dynamic>(query))
            {
                while (resultset.HasMoreResults)
                {
                    FeedResponse<dynamic> response = await resultset.ReadNextAsync();
                    Console.WriteLine("Q1 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                    foreach (var item in response)
                    {
                        list.Add(item);
                    }
                }
            }

            return list.ToArray();
        }

        /// <summary>
        /// Query 2. Cross partition query which is very fast with <10 physical partitions.
        /// </summary>
        /// <param name="ParticipantId"></param>
        /// <returns></returns>
        public async Task<dynamic[]> Q2ViewAllEventsInYear(int ParticipantId)
        {
            if (_container == null)
                await this.Initialize();

            List<dynamic> list = new List<dynamic>();


            QueryDefinition query = new QueryDefinition("SELECT c.Eventname FROM c WHERE c.Eventdate > @Eventdate1 AND c.Eventdate < @Eventdate2 AND c.ParticipantId = @ParticipantId GROUP BY c.Eventname ")
                .WithParameter("@Eventdate1", DateTime.Parse("2020-12-31"))
                .WithParameter("@Eventdate2", DateTime.Parse("2022-1-1"))
                .WithParameter("@ParticipantId", ParticipantId);

            using (FeedIterator<dynamic> resultset = _container.GetItemQueryIterator<dynamic>(query))
            {
                while (resultset.HasMoreResults)
                {
                    FeedResponse<dynamic> response = await resultset.ReadNextAsync();
                    Console.WriteLine("Q2 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                    foreach (var item in response)
                    {
                        list.Add(item);
                    }
                }
            }

            return list.ToArray();
        }

        /// <summary>
        /// Query 3 not optimized. 
        /// </summary>
        /// <param name="eventName"></param>
        /// <returns></returns>
        public async Task<dynamic[]> Old_Q3ViewParticipantsPerEvent(string eventName)
        {
            if (_container == null)
                await this.Initialize();

            List<dynamic> list = new List<dynamic>();

            QueryDefinition query = new QueryDefinition("SELECT c.ParticipantFirstname, c.ParticipantLastname, c.ParticipantId  FROM c WHERE c.Eventname = @Eventname")
                .WithParameter("@Eventname", eventName);

            string continuationToken = string.Empty;
            using (FeedIterator<dynamic> resultset = _container.GetItemQueryIterator<dynamic>(query))
            {
                FeedResponse<dynamic> response = await resultset.ReadNextAsync();
                Console.WriteLine("Q3 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                foreach (var item in response)
                {
                    list.Add(item);
                }
            }

            return list.ToArray();
        }

        /// <summary>
        /// Query 3 optimized by using a continutation token. In a real application this token should be cached and used every time a user clicks next to get the next 100 results.
        /// </summary>
        /// <param name="eventName"></param>
        /// <returns></returns>
        public async Task<dynamic[]> New_Q3ViewParticipantsPerEvent(string eventName)
        {
            if (_container == null)
                await this.Initialize();

            List<dynamic> list = new List<dynamic>();

            QueryDefinition query = new QueryDefinition("SELECT c.ParticipantFirstname, c.ParticipantLastname, c.ParticipantId  FROM c WHERE c.Eventname = @Eventname")
                .WithParameter("@Eventname", eventName);

            string continuationToken = string.Empty;
            using (FeedIterator<dynamic> resultset = _container.GetItemQueryIterator<dynamic>(query, null, new QueryRequestOptions() { MaxItemCount = 100 }))
            {
                FeedResponse<dynamic> response = await resultset.ReadNextAsync();
                Console.WriteLine("Q3 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                foreach (var item in response)
                {
                    list.Add(item);
                }

                continuationToken = response.ContinuationToken;
            }

            using (FeedIterator<dynamic> resultset = _container.GetItemQueryIterator<dynamic>(query, continuationToken, new QueryRequestOptions() { MaxItemCount = 100 }))
            {
                FeedResponse<dynamic> response = await resultset.ReadNextAsync();
                Console.WriteLine("Q3 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                foreach (var item in response)
                {
                    list.Add(item);
                }

                continuationToken = response.ContinuationToken;
            }

            return list.ToArray();
        }

        /// <summary>
        /// Query 4 not optimized. Single item query always takes around 3 RU.
        /// </summary>
        /// <param name="eventName"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task New_Q4ViewHighScoreForParticipant(string eventName, string id)
        {
            if (_container == null)
                await this.Initialize();

            ItemResponse<Marathon> response = await _container.ReadItemAsync<Marathon>(id, new PartitionKey(eventName));

            Console.WriteLine("Q4 took {0} ms. RU consumed: {1}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge);
        }

        /// <summary>
        /// Qyery 4 optimized by using a point read which returns a single item and consumes always 1 RU.
        /// </summary>
        /// <param name="eventName"></param>
        /// <param name="ParticipantId"></param>
        /// <returns></returns>
        public async Task<Marathon[]> Old_Q4ViewHighScoreForParticipant(string eventName, int ParticipantId)
        {
            if (_container == null)
                await this.Initialize();

            List<Marathon> list = new List<Marathon>();

            QueryDefinition query = new QueryDefinition("SELECT c.id, c.ParticipantFirstname, c.ParticipantLastname, c.TotalScore FROM c WHERE c.Eventname = @Eventname AND c.ParticipantId  = @ParticipantId")
                .WithParameter("@Eventname", eventName)
                .WithParameter("@ParticipantId", ParticipantId);

            using (FeedIterator<Marathon> resultset = _container.GetItemQueryIterator<Marathon>(query))
            {
                while (resultset.HasMoreResults)
                {
                    FeedResponse<Marathon> response = await resultset.ReadNextAsync();
                    Console.WriteLine("Q4 took {0} ms. RU consumed: {1}, Number of items : {2}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge, response.Count);

                    foreach (var item in response)
                    {
                        list.Add(item);
                    }
                }
            }

            return list.ToArray();
        }

        /// <summary>
        /// Helper function to display execution time and RU's.
        /// </summary>
        /// <param name="response"></param>
        private void PrintStats(Response<dynamic> response)
        {
            Console.WriteLine("Operation took {0} ms. RU consumed: {1}", response.Diagnostics.GetClientElapsedTime().TotalMilliseconds, response.RequestCharge);
        }

        /// <summary>
        /// Function to optimized the index policy
        /// </summary>
        /// <returns></returns>
        public async Task SetIndexPolicy()
        {
            if (_container == null)
                await this.Initialize();

            ContainerResponse resp = await _container.ReadContainerAsync();

            IndexingPolicy policy = new IndexingPolicy();
            policy.Automatic = true;
            policy.IndexingMode = IndexingMode.Consistent;
            policy.ExcludedPaths.Add(new ExcludedPath { Path = "/*" });
            policy.IncludedPaths.Add(new IncludedPath { Path = "/Eventname/?" });
            policy.IncludedPaths.Add(new IncludedPath { Path = "/Eventdate/?" });
            policy.IncludedPaths.Add(new IncludedPath { Path = "/ParticipantId/?" });
            policy.IncludedPaths.Add(new IncludedPath { Path = "/TotalScore/?" });

            resp.Resource.IndexingPolicy = policy;

            await _container.ReplaceContainerAsync(resp.Resource);
        }
    }
}
