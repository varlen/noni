using System;
using System.Data.Common;
using System.CommandLine.DragonFruit;

using Npgsql;

using noni.Contracts;
using noni.Models;
using noni.Implementations;

namespace noni
{
    class Program
    {
        DatabaseStructure databaseStructure;

        ITypeMapper typeMapper; 
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

            
            DbConnection connection = ConnectTo(sourceDatabase, AcknowledgeDatabase(databaseType));

            Console.WriteLine("Connected");
            
            //IStructureInspector representationExtractor = GetStructureInspector();

            // Extract tables/columns/types information (representation)
            //DatabaseStructure structure = representationExtractor.GetDatabaseStructure(connection);

            // Extract statistics information for numeric columns

            // Classify textual information 

            
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

        public static IStructureInspector GetStructureInspector() {
            return new PostgresStructureInspector();
        }
    }
}
