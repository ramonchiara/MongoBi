using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Bson.Serialization;
using System;
using System.Collections.Generic;

namespace MongoBI
{
	class MainClass
	{
		public static void Main(string[] args)
		{
			BsonClassMap.RegisterClassMap<Anuncio>();

			MongoClient mongo = new MongoClient("mongodb://localhost:27017");
			IMongoDatabase db = mongo.GetDatabase("piv");
			IMongoCollection<Anuncio> table = db.GetCollection<Anuncio>("anuncios");

			BsonDocument filtro1 = new BsonDocument();
			FilterDefinition<Anuncio> filtro2 = FilterDefinition<Anuncio>.Empty;
			FilterDefinition<Anuncio> filtro3 = Builders<Anuncio>.Filter.Exists("preco", false);
			FilterDefinition<Anuncio> filtro4 = Builders<Anuncio>.Filter.Lte("preco", 10000);

			SortDefinition<Anuncio> ordem4 = Builders<Anuncio>.Sort.Descending("preco");
			SortDefinition<Anuncio> ordem5 = Builders<Anuncio>.Sort.Descending("preco").Ascending("cep");

			List<Anuncio> dados1 = table.Find(filtro1).ToList();
			List<Anuncio> dados2 = table.Find(filtro2).ToList();
			List<Anuncio> dados3 = table.Find(filtro3).Skip(5).Limit(5).ToList();
			List<Anuncio> dados4 = table.Find(filtro4).Sort(ordem4).ToList();

			Anuncio dado = table.Find(new BsonDocument("cep", "04144-110")).FirstOrDefault();

			List<Anuncio> anuncios = table.Find("{ preco: { $lt: 1000 } }").Sort("{ preco: -1, cep: 1 }").ToList();

			Show(anuncios);
		}

		public static void Show(List<Anuncio> anuncios)
		{
			Console.WriteLine("Foram encontrados {0} registros", anuncios.Count);
			foreach (Anuncio anuncio in anuncios)
			{
				Console.WriteLine("Id: {0}, Preço: {1}, CEP: {2}", anuncio.id, anuncio.preco, anuncio.cep);
			}
		}

	}

}
