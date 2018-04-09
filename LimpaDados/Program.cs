using System;
using System.Collections.Generic;
using System.Globalization;
using MongoDB.Bson;
using MongoDB.Driver;

namespace LimpaDados
{
    class Program
    {
        static void Main(string[] args)
        {
            MongoClient mongo = new MongoClient("mongodb://localhost:27017");
            IMongoDatabase db = mongo.GetDatabase("piv");
            IMongoCollection<BsonDocument> table = db.GetCollection<BsonDocument>("anuncios");

            List<BsonDocument> anuncios = table.Find("{}").ToList();
            foreach (BsonDocument anuncio in anuncios)
            {
                CleanBreadcrumb(table, anuncio);
                CleanData(table, anuncio);
                CleanPreco(table, anuncio);
                CleanDetalhe(table, anuncio, "Condomínio:");
                CleanDetalhe(table, anuncio, "IPTU:");
            }
        }

        private static void CleanBreadcrumb(IMongoCollection<BsonDocument> table, BsonDocument anuncio)
        {
            BsonType tipo = anuncio["breadcrumb"].BsonType;
            if (tipo == BsonType.String)
            {
                string valorAntigo = anuncio["breadcrumb"].AsString;
                string[] valor = valorAntigo.Split("»");
                for (int i = 0; i < valor.Length; i++)
                {
                    valor[i] = valor[i].Trim();
                }

                BsonDocument valorNovo = new BsonDocument("breadcrumb", new BsonArray(valor));
                table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$set", valorNovo));
            }
        }

        private static void CleanData(IMongoCollection<BsonDocument> table, BsonDocument anuncio)
        {
            BsonType tipo = anuncio["data"].BsonType;
            if (tipo == BsonType.String)
            {
                string valorAntigo = anuncio["data"].AsString;
                valorAntigo = valorAntigo.Replace("Inserido em: ", "");
                valorAntigo = valorAntigo.Replace("Anúncio de Empresa | ", "");
                valorAntigo = valorAntigo.Replace("às ", "");
                DateTime valor = DateTime.ParseExact(valorAntigo, "d MMMM HH:mm", new CultureInfo("pt-BR"));

                BsonDocument valorNovo = new BsonDocument("data", new BsonDateTime(valor));
                table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$set", valorNovo));
            }
        }

        private static void CleanPreco(IMongoCollection<BsonDocument> table, BsonDocument anuncio)
        {
            if (!anuncio.Contains("preco"))
            {
                return;
            }

            BsonType tipo = anuncio["preco"].BsonType;
            if (tipo == BsonType.Int32)
            {
                int valorAntigo = anuncio["preco"].AsInt32;
                double valor = Convert.ToDouble(valorAntigo);

                BsonDocument valorNovo = new BsonDocument("preco", new BsonDouble(valor));
                table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$set", valorNovo));
            }
        }

        private static void CleanDetalhe(IMongoCollection<BsonDocument> table, BsonDocument anuncio, string propriedade)
        {
            if (!anuncio.Contains("detalhes"))
            {
                return;
            }

            BsonDocument detalhes = anuncio["detalhes"].AsBsonDocument;
            if (!detalhes.Contains(propriedade))
            {
                return;
            }

            BsonType tipo = detalhes[propriedade].BsonType;
            if (tipo == BsonType.String)
            {
                string valorAntigo = detalhes[propriedade].AsString;
                valorAntigo = valorAntigo.Replace("R$ ", "");
                double valor = double.Parse(valorAntigo);

                BsonDocument valorNovo = new BsonDocument("detalhes." + propriedade, new BsonDouble(valor));
                if (valor > 0)
                {
                    table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$set", valorNovo));
                }
                else
                {
                    table.UpdateOne(new BsonDocument("_id", anuncio["_id"]), new BsonDocument("$unset", valorNovo));
                }
            }
        }
    }
}
