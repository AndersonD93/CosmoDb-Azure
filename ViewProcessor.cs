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
using Microsoft.AspNetCore.JsonPatch.Internal;
using System.Reflection;
using Microsoft.Azure.Documents.SystemFunctions;

namespace Azure.Samples.Processor
{   
    public class ViewProcessor
    {
        private DocumentClient _client;
        private Uri _collectionUri;
        private Uri _collectionUri2;
        private ILogger _log;
        

        private string _databaseName = Environment.GetEnvironmentVariable("DatabaseName1");
        private string _collectionName = Environment.GetEnvironmentVariable("ViewCollectionName1");
        private string _collectionName2 = Environment.GetEnvironmentVariable("ViewCollectionName2");


        public ViewProcessor(DocumentClient client, ILogger log)
        {
            _log = log;
            _client = client;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(_databaseName, _collectionName);
            _collectionUri2 = UriFactory.CreateDocumentCollectionUri(_databaseName, _collectionName2);
        }

        public async Task UpdateHeroMaterializedView(Hero hero)
        {
            _log.LogInformation("Updating Hero materialized view");

            Document viewAll = null;

            //Obtiene o establece PartitionKey para la solicitud actual en el servicio Azure Cosmos DB.
            var optionsAll = new RequestOptions() { PartitionKey = new PartitionKey("id") };

            int attempts = 0;

            while (attempts < 10)
            {
                try
                {
                    var uriAll = UriFactory.CreateDocumentUri(_databaseName, _collectionName, "id");

                    _log.LogInformation($"Materialized view: {uriAll.ToString()}");

                    viewAll = await _client.ReadDocumentAsync(uriAll, optionsAll);
                }
                catch (DocumentClientException ex)
                {
                    if (ex.StatusCode != HttpStatusCode.NotFound)
                        throw ex;
                }

                if (viewAll == null)
                {
                    viewAll = new Document();
                    viewAll.SetPropertyValue("NumeroIdentificacion", "id");
                    viewAll.SetPropertyValue("name", "id");
                    viewAll.SetPropertyValue("owner", "id");
                }

                //Obtiene o establece la condición (ETag) asociada a la solicitud en el servicio Azure Cosmos DB.

                AccessCondition acAll = new AccessCondition()
                {
                    Type = AccessConditionType.IfMatch,
                    Condition = viewAll.ETag
                };
                optionsAll.AccessCondition = acAll;

                try
                {
                    await UpsertDocument(viewAll, optionsAll);
                    return;
                }
                catch (DocumentClientException de)
                {
                    if (de.StatusCode == HttpStatusCode.PreconditionFailed)
                    {
                        attempts += 1;
                        _log.LogWarning($"Optimistic concurrency pre-condition check failed. Trying again ({attempts}/10)");
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            throw new ApplicationException("Could not insert document after retring 10 times, due to concurrency violations");
        }

        public async Task UpdateHeroMaterializedView1(Hero hero)
        {
            var optionsSingle = new RequestOptions() { PartitionKey = new PartitionKey(hero.id) };

            HeroMaterializedView viewSingle = null;

            try
            {
                var uriSingle = UriFactory.CreateDocumentUri(_databaseName, _collectionName, hero.id);

                _log.LogInformation($"Materialized view: {uriSingle.ToString()}");

                viewSingle = await _client.ReadDocumentAsync<HeroMaterializedView>(uriSingle, optionsSingle);
            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode != HttpStatusCode.NotFound)
                    throw ex;
            }

            //log.LogInformation("Document: " + viewSingle.ToString());

            if (viewSingle == null)
            {
                _log.LogInformation("Creating new materialized view :");
                viewSingle = new HeroMaterializedView()
                {
                    id = hero.id,
                    name = hero.name,
                    owner = hero.owner,
                };
            }
            else
            {
                _log.LogInformation("Updating materialized view");
                viewSingle.id = hero.id;
                viewSingle.name = hero.name;
                viewSingle.owner = hero.owner;
            }

            await UpsertDocument(viewSingle, optionsSingle);
        }

        public async Task UpdateHeroActorsMaterializedView1(Actors actors)
        {
            var optionsSingle = new RequestOptions() { PartitionKey = new PartitionKey(actors.id) };

            HeroActorsMaterializedView viewSingle = null;

            try
            {
                var uriSingle = UriFactory.CreateDocumentUri(_databaseName, _collectionName2, actors.id);

                _log.LogInformation($"Materialized view: {uriSingle.ToString()}");

                viewSingle = await _client.ReadDocumentAsync<HeroActorsMaterializedView>(uriSingle, optionsSingle);

            }
            catch (DocumentClientException ex)
            {
                if (ex.StatusCode != HttpStatusCode.NotFound)
                    throw ex;
            }

            Actors data1 = new Actors();

            Datos data = JsonConvert.DeserializeObject<Datos>(actors.datos1);
            data1.datos = data.datos;


            if (viewSingle == null)
            {
                _log.LogInformation("Creating new materialized view :");
                List<ObjAtributosActor> list = new List<ObjAtributosActor>();
                foreach (var item in data1.datos)
                {
                   
                    ObjAtributosActor atributos = new ObjAtributosActor();
                    atributos.actorname = item.actorname;
                    atributos.direccion = item.direccion;

                    list.Add(atributos);

                }
                viewSingle = new HeroActorsMaterializedView()
                {
                    id = actors.id,
                    datos = list
                };

            }
            else
            {
                _log.LogInformation("Updating materialized view");
                viewSingle.id = actors.id;
                List<ObjAtributosActor> list = new List<ObjAtributosActor>();
                foreach (var item in data1.datos)
                {

                    ObjAtributosActor atributos = new ObjAtributosActor();
                    atributos.actorname = item.actorname;
                    atributos.direccion = item.direccion;

                    list.Add(atributos);

                }
                viewSingle.datos = list;
               
            }

            await UpsertDocument(viewSingle, optionsSingle);
        }

        public object GetPropertyValue(object obj, string propertyName)
        {
            var _propertyNames = propertyName.Split('.');

            for (var i = 0; i < _propertyNames.Length; i++)
            {
                if (obj != null)
                {
                    var _propertyInfo = obj.GetType().GetProperty(_propertyNames[i]);
                    if (_propertyInfo != null)
                        obj = _propertyInfo.GetValue(obj);
                    else
                        obj = null;
                }
            }

            return obj;
        }


        private async Task<ResourceResponse<Document>> UpsertDocument(object document, RequestOptions options)
        {
            int attempts = 0;

            while (attempts < 3)
            {
                try
                {   //Inserta un documento como una operación asincrónica en el servicio Azure Cosmos DB.
                    var result = await _client.UpsertDocumentAsync(_collectionUri2, document, options);                      
                    _log.LogInformation($"{options.PartitionKey} RU Used: {result.RequestCharge:0.0}");
                    return result;                                  
                }
                catch (DocumentClientException de)
                {
                    if (de.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        _log.LogWarning($"Waiting for {de.RetryAfter} msec...");
                        await Task.Delay(de.RetryAfter);
                        attempts += 1;
                    }
                    else
                    {
                        throw;
                    }
                }
            }            

            throw new ApplicationException("Could not insert document after being throttled 3 times");
        }
    }
}
