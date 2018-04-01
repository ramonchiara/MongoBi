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

            List<BsonDocument> resultado = table.Aggregate()
                                                .Group("{_id: '$data', res: {$sum: 1}}")
                                                .Project("{_id: 1, res: 1}")
                                                .Sort("{_id: 1}")
                                                .ToList();
            foreach (BsonDocument d in resultado)
            {
                Console.WriteLine(d);
            }

            List<BsonDocument> minmax = table.Aggregate()
                                             .Match("{preco: {$exists: 1}}")
                                             .Group("{_id: null, min: {$min: '$preco'}, max: {$max: '$preco'}}")
                                             .ToList();

            double min = Convert.ToDouble(minmax[0]["min"]);
            double max = Convert.ToDouble(minmax[0]["max"]);

            List<BsonDocument> mediavar = table.Aggregate()
                                               .Match("{preco: {$exists: 1}}")
                                               .Group("{_id: null, media: {$avg: '$preco'}, values: {$push: '$preco'}}")
                                               .Unwind("values")
                                               .Project("{media: 1, delta: {$subtract: ['$values', '$media']}}")
                                               .Project("{media: 1, delta: {$multiply: ['$delta', '$delta']}}")
                                               .Group("{_id: null, media: {$avg: '$media'}, s2: {$avg: '$delta'}}")
                                              .ToList();

            double media = Convert.ToDouble(mediavar[0]["media"]);
            double s2 = Convert.ToDouble(mediavar[0]["s2"]);
            double s = Math.Sqrt(s2);

            Console.WriteLine("Menor preço: {0:F2}", min);
            Console.WriteLine("Maior preço: {0:F2}", max);
            Console.WriteLine("Média dos preços: {0:F2}", media);
            Console.WriteLine("Variância: {0:F2}", s2);
            Console.WriteLine("Desvio padrão: {0:F2}", s);

            List<Anuncio> precos = table.Find("{preco: {$exists: 1}}").Sort("{preco: 1}").ToList();
            int meio = precos.Count / 2;

            double mediana = 0;
            if (precos.Count % 2 == 0)
            {
                double p1 = precos[meio].preco.Value;
                double p2 = precos[meio + 1].preco.Value;
                mediana = (p1 + p2) / 2;
            }
            else
            {
                mediana = precos[meio + 1].preco.Value;
            }

            Console.WriteLine("Mediana: {0:F2}", mediana);
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
