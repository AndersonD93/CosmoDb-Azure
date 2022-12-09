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
using System.Reflection;

namespace Azure.Samples.Entities
{
 
    public class Hero
    {
        public string id;
        public string name;
        public string owner;

        public static Hero FromDocument(Document document)
        {
            var result = new Hero()
            {
                id = document.GetPropertyValue<string>("id"),
                name = document.GetPropertyValue<string>("name"),
                owner = document.GetPropertyValue<string>("owner"),
            };

            return result;
        }

    }


    public class HeroMaterializedView
    {
        [JsonProperty("id")]
        public string id;

        [JsonProperty("name")]
        public string name;

        [JsonProperty("owner")]
        public string owner;
        internal object datos;
    }

    public class HeroActorsMaterializedView
    {
        [JsonProperty("id")]
        public string id;

        [JsonProperty("actors")]
        public string [] actors;

    }

    public class Datos
    {
        public List<ObjAtributosActor> datos { get; set; }
    }

    public class ObjAtributosActor
    {
        public string actorname { get; set; }
        public string direccion { get; set; }

    }

    public class Actors
    {
        public string id { get; set; } //clave de particion
        public List<ObjAtributosActor> datos { get; set; }
        public string datos1;

        public static Actors FromDocument(Document document)
        {
            var actors1 = document.GetPropertyValue<Object>("actors");

            var result = new Actors()
            {
                id = document.GetPropertyValue<string>("id"),
                datos1 = JsonConvert.SerializeObject(actors1)

            };
            return result;
        }

    }
}
