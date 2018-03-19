using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

using System;

namespace MongoBI
{
    [BsonIgnoreExtraElements]
    public class Anuncio
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string oid { get; set; }
        public string url { get; set; }
        public string id { get; set; }
        public string titulo { get; set; }
        // [BsonIgnore]
        [BsonIgnoreIfNull]
        public double? preco { get; set; }
        // public string precoAsString { get; set; }
        public string cep { get; set; }
        [BsonElement("baixo")]
        public bool precoBaixo { get; set; }

		public override string ToString()
		{
			return string.Format("[Anuncio: oid={0}, url={1}, id={2}, titulo={3}, preco={4}, cep={5}, precoBaixo={6}]", oid, url, id, titulo, preco, cep, precoBaixo);
		}
    }
}
