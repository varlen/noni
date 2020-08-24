using System;
using System.Data.Common;
using System.Data;
using Npgsql;

using noni.Contracts;
using noni.Models;

namespace noni.Implementations {

    public class PostgresDatabaseMetadataCollector : IDatabaseMetadataCollector {

        public DatabaseStructure Collect(DbConnection connection, DatabaseStructure dbStructure)
        {
            foreach (TableDescription table in dbStructure.GetTables().Values)
            {
                foreach (var column in table.columns)
                {
                    // Process column
                    ApplyColumnMetadata(connection, column, table);
                }
            }
            return dbStructure;
        }

        private void ApplyColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            column.metadata = column.type switch
            {
                AgnosticColumnType.Integer => GetNumericColumnMetadata(connection, column, table),
                AgnosticColumnType.Decimal => GetNumericColumnMetadata(connection, column, table),
                AgnosticColumnType.Bool => null,
                AgnosticColumnType.Key => null,
                AgnosticColumnType.Time => null, // TODO
                AgnosticColumnType.Text => null, // TODO
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
                metadata.distinct = (int) cmd.ExecuteScalar();
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
                metadata = new NumericColumnMetadata(defaultMetadata)
                {
                    max = reader.GetFieldValue<Decimal>("max"),
                    min = reader.GetFieldValue<Decimal>("min"),
                    variance = reader.GetFieldValue<Decimal>("variance"),
                    average = reader.GetFieldValue<Decimal>("average")
                };
            }

            var modeQuery = String.Format(
                @"SELECT mode() WITHIN GROUP (ORDER BY {0}) As modal FROM {1}.{2}",
                column.name, table.schema, table.name);
            
            using (var cmd = new NpgsqlCommand(modeQuery, (NpgsqlConnection) connection))
            {
                metadata.mode = (Decimal) cmd.ExecuteScalar();
            }

            return metadata;
        }

        

    }

}