using System;
using System.Data.Common;
using System.CommandLine.DragonFruit;
using System.IO;
using System.Diagnostics;

using Npgsql;

using noni.Contracts;
using noni.Models;
using noni.Implementations;

namespace noni
{
    class Program
    {
        DatabaseStructure databaseStructure;

        // Process the database according to the structure, match patterns and extract statistic data from column agreggations
        // Produces metadata that composes the register

        IRegisterSource registerSource;  // Contains all the information necessary to create anonymized registries
        

        /// <summary>
        ///
        /// </summary>
        /// <param name="databaseType"> Database type. E.g.: postgres, sqlserver, mysql</param>
        /// <param name="sourceDatabase"> Source database connection string</param>
        /// <param name="outputFile"> Output file name</param>
        public static void Main(String sourceDatabase = null, String databaseType = "postgres", String outputFile = "output.json")
        {
            if (sourceDatabase == null) {
                Console.WriteLine("Please provide the source database connection string.");
                return;
            }

            if (databaseType == null) {
                Console.WriteLine("Please provide the database type.");
                return;
            }

            KnownDatabase knownDatabase = AcknowledgeDatabase(databaseType);
            if (knownDatabase == KnownDatabase.Other) {
                Console.WriteLine("No implementation avaliable for database '{0}'", databaseType);
                return;
            }
            Stopwatch timer = new Stopwatch();
            timer.Start();

            DbConnection connection = ConnectTo(sourceDatabase, knownDatabase);

            Console.WriteLine("Connected");
            
            IStructureInspector representationExtractor = GetStructureInspector(knownDatabase);
            IDatabaseMetadataCollector metadataCollector = GetDatabaseMetadataCollector(knownDatabase);

            // Extract tables/columns/types information (representation)
            DatabaseStructure structure = representationExtractor.GetDatabaseStructure(connection);

            string serializedStructure = structure.ToString();
            
            // Extract statistics information for numeric columns and classify textual information 
            DatabaseStructure structureWithMetadata = metadataCollector.Collect(connection, structure);
 
            string serializedWithMetadata = structureWithMetadata.ToString();

            // Serialize structure to file
            File.WriteAllText(outputFile, serializedStructure);
            File.WriteAllText("serial_" + outputFile, serializedWithMetadata);

            timer.Stop();
            TimeSpan ts = timer.Elapsed;
            string elapsedTime = String.Format("{0:00}:{1:00}:{2:00}.{3:00}",
            ts.Hours, ts.Minutes, ts.Seconds,
            ts.Milliseconds / 10);
            Console.WriteLine("Done. Elapsed time: {0}", elapsedTime);
        }

        public static DbConnection ConnectTo(String connectionString, KnownDatabase database) {

            if (database == KnownDatabase.Postgres) {
                var conn = new NpgsqlConnection(connectionString); 
                conn.Open();
                return conn;                
            } else {
                throw new NotImplementedException();
            }

        }

        public static KnownDatabase AcknowledgeDatabase(String databaseType) {
            if (databaseType.ToLower() == "postgres")
            {
                return KnownDatabase.Postgres;
            }
            return KnownDatabase.Other;
        }

        public static void GenerateRegisterSource(DbConnection connection) {
            throw new NotImplementedException();
        }

        public static IStructureInspector GetStructureInspector(KnownDatabase dbKind) {
            if (dbKind == KnownDatabase.Postgres)
                return new PostgresStructureInspector();
            return null;
        }

        public static IDatabaseMetadataCollector GetDatabaseMetadataCollector(KnownDatabase dbKind) {
            if (dbKind == KnownDatabase.Postgres)
                return new PostgresDatabaseMetadataCollector();
            return null;
        }
    }
}
