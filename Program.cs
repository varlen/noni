using System;
using System.Data.Common;
using System.CommandLine.DragonFruit;

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
        /// <param name="sourceDatabase"> Source database connection string</param>
        /// <param name="outputFile"> Output file name</param>
        public static void Main(String sourceDatabase = null, String outputFile = "output.json")
        {
            if (sourceDatabase == null) {
                Console.WriteLine("Please provide the source database connection string.");
                return;
            }

            DbConnection connection = ConnectTo(sourceDatabase);
            
            IStructureInspector representationExtractor = GetStructureInspector();

            // Extract tables/columns/types information (representation)
            DatabaseStructure structure = representationExtractor.GetDatabaseStructure(connection);

            // Extract statistics information for numeric columns

            // Classify textual information 

            
        }

        public static void PrintHelp() {
            Console.WriteLine("Help");
        }

        public static DbConnection ConnectTo(String connectionString) {
            throw new NotImplementedException();
        }

        public static void GenerateRegisterSource(DbConnection connection) {
            throw new NotImplementedException();
        }

        public static IStructureInspector GetStructureInspector() {
            return new PostgresStructureInspector();
        }
    }
}
