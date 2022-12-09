using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.Threading.Tasks;
using System.Linq;
using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
using Microsoft.Azure.Documents.Client;
using System.Net;
using Azure.Samples.Entities;
using Azure.Samples.Processor;

namespace Azure.Samples
{
    public static class Function
    {
        [FunctionName("HeroMaterializedViewProcessor")]
        public static async Task Run(
            [CosmosDBTrigger(
                databaseName: "%DatabaseName1%", 
                collectionName: "%RawCollectionName1%", 
                ConnectionStringSetting = "cosmoconnectionString",
                LeaseCollectionName = "leases", 
                FeedPollDelay=1000,
                CreateLeaseCollectionIfNotExists = true
            )]IReadOnlyList<Document> input,
            [CosmosDB(
                databaseName: "%DatabaseName1%",
                collectionName: "%ViewCollectionName1%",
                ConnectionStringSetting = "cosmoconnectionString"
            )]DocumentClient client,
            [CosmosDB(
                databaseName: "%DatabaseName1%",
                collectionName: "%ViewCollectionName2%",
                ConnectionStringSetting = "cosmoconnectionString"
            )]DocumentClient client2,
            ILogger log
        )        
        {
            if (input != null && input.Count > 0)
            {
                var p = new ViewProcessor(client, log);
                var p2 = new ViewProcessor(client2, log);

                log.LogInformation($"Processing {input.Count} events");
                
                foreach(var d in input)
                {
                    var hero = Hero.FromDocument(d);
                    var actors = Actors.FromDocument(d);
                    

                    var tasks = new List<Task>();

                    //tasks.Add(p.UpdateHeroMaterializedView(hero));
                    //tasks.Add(p.UpdateHeroMaterializedView1(hero));
                    tasks.Add(p2.UpdateHeroActorsMaterializedView1(actors));

                    await Task.WhenAll(tasks);
                }    
            }
        }
    }
}
