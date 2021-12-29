using System;
using System.Data.Common;
using System.Data;
using System.Text.Json;
using System.Net.Http;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Npgsql;

using noni.Contracts;
using noni.Models;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;

namespace noni.Implementations {

    public class PostgresDatabaseMetadataCollector : IDatabaseMetadataCollector {

        public static Random rnd = new Random();
        public const string SATO_URL = "http://localhost:5000/upload-and-predict";

        public DatabaseStructure Collect(DbConnection connection, DatabaseStructure dbStructure)
        {

            using (var httpClient = new HttpClient())
            {
                foreach (TableDescription table in dbStructure.GetTables().Values)
                {
                    foreach (var column in table.columns)
                    {
                        // Process column
                        ApplyColumnMetadata(connection, column, table);
                    }

                    // Generate CSV from table sample
                    var tableSampleCsvContent = GenerateCsvFromTable(table);

                    // Do HTTP request with tableSampleCsvContent to SATO endpoint
                    var semanticColumnInferences = CallSatoModel(httpClient, table.name, tableSampleCsvContent).GetAwaiter().GetResult();

                    // Map SATO definitions to generators
                    foreach (var column in table.columns)
                    {
                        if (semanticColumnInferences.TryGetValue(column.name, out string satoCategory))
                        {
                            var metadata = (TextColumnMetadata) column.metadata;
                            metadata.satoCategory = satoCategory;

                            // TODO - Depending on the category, TextColumnMetadata may no longer be a proper class to describe it
                        }
                    }
                }
            }
            
            return dbStructure;
        }

        private async Task<Dictionary<string,string>> CallSatoModel(HttpClient client, string tableName, MemoryStream csvContent) 
        {
            if (csvContent.Length == 0) {
                // No csv to send to Sato in this case
                return new Dictionary<string, string>();
            }

            Console.WriteLine($"Calling SATO model for table {tableName}");

            using (var content = new MultipartFormDataContent("Upload----" + DateTime.Now.ToString(CultureInfo.InvariantCulture)))
            {
                var streamContent = new StreamContent(csvContent);
                content.Add(streamContent, tableName, $"{tableName}.csv");

                using (var message = await client.PostAsync(SATO_URL, content))
                {
                    var result = await message.Content.ReadAsStringAsync();
                    var inferredTypes = JsonSerializer.Deserialize<List<string>>(result);
                    csvContent.Position = 0;
                    
                    using (var sr = new StreamReader(csvContent, Encoding.Unicode)) 
                    {
                        var header = sr.ReadLine().Trim();
                        var columns = header.Split(",");

                        Console.WriteLine("headers String: {0}", header);
                        Console.WriteLine("sato results: {0}", result);

                        var satoResult = new Dictionary<string, string>();

                        for (var i = 0; i < inferredTypes.Count; i++)
                        {
                            satoResult.Add(columns[i], inferredTypes[i]);
                        }

                        return satoResult;
                    }
                }

            }
        }

        private MemoryStream GenerateCsvFromTable(TableDescription table) 
        {
            Dictionary<string, List<string>> columnSamples = new Dictionary<string, List<string>>();

            foreach (var column in table.columns)
            {
                if (column.metadata is TextColumnMetadata) {
                    var sampleList = new List<string>();
                    var samples = ((TextColumnMetadata) column.metadata).samples;
                    if (samples != null) 
                    {
                        sampleList.AddRange(samples);
                    }
                    columnSamples.Add(column.name, sampleList);
                }
                
                // Possibly expand to other types of columns here
            }

            int maxSamples;
            if (columnSamples.Count > 0) 
            {
                var samples = columnSamples.Values.Select( x => { return x.Count; });
                if (samples.Count() > 0)
                {
                    maxSamples = samples.Max();
                }
                else
                {
                    maxSamples = 0;
                }
            }
            else
            {
                maxSamples = 0;
            }

            Console.WriteLine("{0} csv rows will be generated for table {1}", maxSamples, table.name);

            var csvMemoryStream = new MemoryStream();

            if (maxSamples == 0)
            {
                return csvMemoryStream;
            }

            using (StreamWriter sw = new StreamWriter(csvMemoryStream, Encoding.Unicode, 2048, true))
            {
                sw.WriteLine(string.Join(',', columnSamples.Keys));

                for (int i = 0; i < maxSamples; i++) {
                    for (int j = 0; j < columnSamples.Keys.Count; j++ ) {

                        var samples = columnSamples.Values.ElementAt(j);
                        var lastColumn = j == columnSamples.Keys.Count - 1;
                        
                        string sample;
                        if (samples.Count == 0) {
                            sample = string.Empty;
                        }
                        else if ( i >= samples.Count ) 
                        {
                            // Index passed the last sample available for this column, repeat a random sample
                            var sampleIndex = rnd.Next(samples.Count);
                            Console.WriteLine($"Picking random sample @ position {sampleIndex}  (total samples: {samples.Count})");
                            sample = samples[sampleIndex];
                        }
                        else
                        {
                            // Pick a sample from column
                            sample = samples[i];
                        }

                        sw.Write(sample.Replace("'","\'"));

                        if (lastColumn) 
                        {
                            sw.WriteLine();
                        }
                        else
                        {
                            sw.Write(",");
                        }
                    }
                }
            }

            csvMemoryStream.Position = 0;
            return csvMemoryStream;
        }

        private void ApplyColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            Console.WriteLine("Extracting column metadata to {0}.{1}", table.name, column.name);
            // https://devblogs.microsoft.com/dotnet/do-more-with-patterns-in-c-8-0/
            column.metadata = column.type switch
            {
                AgnosticColumnType.Integer => GetNumericColumnMetadata(connection, column, table),
                AgnosticColumnType.Decimal => GetNumericColumnMetadata(connection, column, table),
                AgnosticColumnType.Bool => GetBooleanColumnMetadata(connection, column, table),
                AgnosticColumnType.Key => null,
                AgnosticColumnType.Time => GetDateTimeColumnMetadata(connection, column, table),
                AgnosticColumnType.Text => GetTextColumnMetadata(connection, column, table),
                _ => GetDefaultColumnMetadata(connection, column, table)
            };
        }

        private ColumnMetadata GetDefaultColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            var metadata = new ColumnMetadata();
            var countQuery = String.Format(
                @"SELECT DISTINCT count({0}) FROM {1}.{2}",
                column.name, table.schema, table.name);
            
            using (var cmd = new NpgsqlCommand(countQuery, (NpgsqlConnection) connection))
            {
                metadata.distinct = (Int64) cmd.ExecuteScalar();
            }
            return metadata;
        }

        private CategoricColumnMetadata GetBooleanColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            var metadata = new CategoricColumnMetadata();
            metadata.entityType = NamedEntity.Boolean;

            var trueQuery = String.Format(
                @"SELECT count({0}) FROM {1}.{2} WHERE {0} = 1",
                column.name, table.schema, table.name
            );

            var falseQuery = String.Format(
                @"SELECT count({0}) FROM {1}.{2} WHERE {0} = 0",
                column.name, table.schema, table.name
            );

            using (var cmd = new NpgsqlCommand(trueQuery, (NpgsqlConnection) connection))
            {
                var trueCount = (Int64) cmd.ExecuteScalar();
                metadata.categories.Add( true, trueCount );
            }

            using (var cmd = new NpgsqlCommand(falseQuery, (NpgsqlConnection) connection))
            {
                var falseCount = (Int64) cmd.ExecuteScalar();
                metadata.categories.Add( false, falseCount );
            }

            return metadata;
        }

        private NumericColumnMetadata GetNumericColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            ColumnMetadata defaultMetadata = GetDefaultColumnMetadata(connection, column, table);
            // Extract variance, max, min, mean, mode values
            // https://www.postgresql.org/docs/9.1/functions-aggregate.html
            NumericColumnMetadata metadata;
            var numericStatQuery = String.Format(@"SELECT avg({0}) as average,
                            max({0}) as max,
                            min({0}) as min,
                            variance({0}) as variance
                            FROM {1}.{2}", 
                            column.name, table.schema, table.name);

            using (var cmd = new NpgsqlCommand(numericStatQuery, (NpgsqlConnection) connection))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.HasRows && reader.Read()) 
                {
                    metadata = new NumericColumnMetadata(defaultMetadata)
                    {
                        max = Convert.ToDecimal(reader.GetValue("max")),
                        min = Convert.ToDecimal(reader.GetValue("min")),
                        variance = Convert.ToDecimal(reader.GetValue("variance")),
                        average = Convert.ToDecimal(reader.GetValue("average"))
                    };
                }
                else
                {
                    metadata = new NumericColumnMetadata(defaultMetadata);
                }
            }

            var modeQuery = String.Format(
                @"SELECT mode() WITHIN GROUP (ORDER BY {0}) As modal FROM {1}.{2}",
                column.name, table.schema, table.name);
            
            using (var cmd = new NpgsqlCommand(modeQuery, (NpgsqlConnection) connection))
            {
                metadata.mode = Convert.ToDecimal(cmd.ExecuteScalar());
            }

            return metadata;
        }

        private DateTimeColumnMetadata GetDateTimeColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            ColumnMetadata defaultMetadata = GetDefaultColumnMetadata(connection, column, table);
            // Extract variance, max, min, mean, mode values
            // https://www.postgresql.org/docs/9.1/functions-aggregate.html
            DateTimeColumnMetadata metadata;
            var numericStatQuery = String.Format(@"SELECT
                            max({0}) as max,
                            min({0}) as min
                            FROM {1}.{2}", 
                            column.name, table.schema, table.name);

            using (var cmd = new NpgsqlCommand(numericStatQuery, (NpgsqlConnection) connection))
            using (var reader = cmd.ExecuteReader())
            {
                if (reader.Read()) {
                    metadata = new DateTimeColumnMetadata(defaultMetadata)
                    {
                        max = reader.GetFieldValue<DateTime>("max"),
                        min = reader.GetFieldValue<DateTime>("min")
                    };
                } else {
                    metadata = new DateTimeColumnMetadata(defaultMetadata);
                }

            }

            var modeQuery = String.Format(
                @"SELECT mode() WITHIN GROUP (ORDER BY {0}) As modal FROM {1}.{2}",
                column.name, table.schema, table.name);
            
            using (var cmd = new NpgsqlCommand(modeQuery, (NpgsqlConnection) connection))
            {
                metadata.mode = (DateTime) cmd.ExecuteScalar();
            }

            return metadata;
        }

        private ColumnMetadata GetTextColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            ColumnMetadata defaultMetadata = GetDefaultColumnMetadata(connection, column, table);
            var metadata = new TextColumnMetadata(defaultMetadata);

            var countQuery = String.Format(
                @"SELECT count({0}) as count FROM {1}.{2}", 
                column.name, table.schema, table.name
            );

            Int64 rowCount = 0;
            using (var cmd = new NpgsqlCommand(countQuery, (NpgsqlConnection) connection))
            {
                rowCount = (Int64) cmd.ExecuteScalar();
            }
            


            if (column.nativeType == "text")
            {
                return metadata;
            }
            
            List<String> samples = new List<String>();
            if (rowCount > 0) 
            {
                // TODO - Make it configurable
                var rowsToSample = 100;
                metadata.sampleCount = rowsToSample;

                var proportion = 100 * ( rowsToSample  / rowCount );

                if (proportion > 100)
                {
                    proportion = 100;
                }

                if (proportion < 1)
                {
                    proportion = 1;
                }

                // Postgres 9.5+
                var dataSampleQuery = String.Format(
                    @"SELECT {0} FROM {1}.{2} TABLESAMPLE SYSTEM ({3}) WHERE {0} IS NOT NULL LIMIT {4}",
                    column.name, table.schema, table.name, proportion, rowsToSample);

                using (var cmd = new NpgsqlCommand(dataSampleQuery, (NpgsqlConnection) connection))
                using (var reader = cmd.ExecuteReader())
                {
                    while (reader.HasRows && reader.Read())
                    {
                        samples.Add(reader.GetString(0));
                    }
                }

                metadata.samples = samples.Distinct().ToList();
                metadata.sampleCount = samples.Count;

                Console.WriteLine(" Testing textual category");
                metadata.entityType = Common.EntityMatcher.Match(samples);
            }
            

            if (metadata.entityType == NamedEntity.Unknown)
            {
                var samplesWithCommas = String.Join(",", samples);

                Console.WriteLine(" Could not identify textual data type. Examples:\n " + samplesWithCommas);

                var categoryDict = CategoricColumnMetadata.TryCategoriesFromSamples(samples);

                if (categoryDict.Keys.Count == samples.Count)
                {
                    Console.WriteLine(" Data is probably not categoric. Using default metadata");
                    return metadata;
                }
                else
                {
                    return new CategoricColumnMetadata(categoryDict);
                }
            }
            return metadata;
        }

    }

}