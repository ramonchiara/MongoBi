using System;
using System.Collections.Generic;
using MongoDB.Bson;
using MongoDB.Driver;

namespace KMeans
{
    class Program
    {
        static void Main(string[] args)
        {
            int k = 4;

            MongoClient mongo = new MongoClient("mongodb://localhost:27017");
            IMongoDatabase db = mongo.GetDatabase("piv");
            IMongoCollection<BsonDocument> table = db.GetCollection<BsonDocument>("anuncios");

            LimpaClasses(table);
            IniciaClasses(k, table);

            List<BsonDocument> centros, centrosAntigos = null;
            bool movimentou = true;
            do
            {
                centros = CalculaCentros(table);
                foreach (BsonDocument centro in centros) { Console.WriteLine(centro); }
                movimentou = HouveMovimento(centros, centrosAntigos);
                AtualizaClasses(table, centros);
                centrosAntigos = centros;
            } while (movimentou);
        }

        private static void LimpaClasses(IMongoCollection<BsonDocument> table)
        {
            BsonDocument classe = new BsonDocument("classe", new BsonInt32(-1));
            table.UpdateMany("{ classe: { $exists: 1 } }", new BsonDocument("$unset", classe));
        }

        private static void IniciaClasses(int k, IMongoCollection<BsonDocument> table)
        {
            Random rng = new Random();

            List<BsonDocument> anuncios = table.Find("{ preco: { $exists: 1 }, 'detalhes.Condomínio:': { $exists: 1 } }").ToList();
            foreach (BsonDocument anuncio in anuncios)
            {
                int classe = rng.Next(k);
                BsonDocument valorNovo = new BsonDocument("classe", new BsonInt32(classe));
                table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$set", valorNovo));
            }
        }

        private static List<BsonDocument> CalculaCentros(IMongoCollection<BsonDocument> table)
        {
            return table.Aggregate()
                        .Match("{ classe: { $exists: 1 } }")
                        .Group("{ _id: '$classe', preco: { $avg: '$preco' }, condominio: { $avg: '$detalhes.Condomínio:' } }")
                        .Sort("{ _id: 1 }")
                        .ToList();
        }

        private static void AtualizaClasses(IMongoCollection<BsonDocument> table, List<BsonDocument> centros)
        {
            List<BsonDocument> anuncios = table.Find("{ preco: { $exists: 1 }, 'detalhes.Condomínio:': { $exists: 1 } }").ToList();
            foreach (BsonDocument anuncio in anuncios)
            {
                int classe = CalculaMaisProximo(anuncio, centros);
                BsonDocument valorNovo = new BsonDocument("classe", new BsonInt32(classe));
                table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$set", valorNovo));
            }
        }

        private static int CalculaMaisProximo(BsonDocument anuncio, List<BsonDocument> centros)
        {
            int classe = -1;

            double p = anuncio["preco"].AsDouble;
            double c = anuncio["detalhes"]["Condomínio:"].AsDouble;
            double menorDistancia = double.MaxValue;

            for (int i = 0; i < centros.Count; i++)
            {
                double p0 = centros[i]["preco"].AsDouble;
                double c0 = centros[i]["condominio"].AsDouble;
                double distancia = Distancia(p, c, p0, c0);

                if (distancia < menorDistancia)
                {
                    menorDistancia = distancia;
                    classe = i;
                }
            }

            return classe;
        }

        private static double Distancia(double x, double y, double x0, double y0)
        {
            double dx = x - x0;
            double dy = y - y0;

            return Math.Sqrt(dx * dx + dy * dy);
        }

        private static bool HouveMovimento(List<BsonDocument> centros, List<BsonDocument> centrosAntigos)
        {
            bool movimentou = centrosAntigos == null;

            if (centrosAntigos != null)
            {
                for (int i = 0; i < centros.Count; i++)
                {
                    double p0 = centrosAntigos[i]["preco"].AsDouble;
                    double c0 = centrosAntigos[i]["condominio"].AsDouble;

                    double p = centros[i]["preco"].AsDouble;
                    double c = centros[i]["condominio"].AsDouble;

                    if (Distancia(p, c, p0, c0) > 0.01)
                    {
                        movimentou = true;
                        break;
                    }
                }
            }

            return movimentou;
        }
    }
}
