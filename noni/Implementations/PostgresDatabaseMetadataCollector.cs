using System;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;
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
                metadata.distinct = (int) cmd.ExecuteScalar();
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
                var trueCount = (int) cmd.ExecuteScalar();
                metadata.categories.Add( true, trueCount );
            }

            using (var cmd = new NpgsqlCommand(falseQuery, (NpgsqlConnection) connection))
            {
                var falseCount = (int) cmd.ExecuteScalar();
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

        private DateTimeColumnMetadata GetDateTimeColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            ColumnMetadata defaultMetadata = GetDefaultColumnMetadata(connection, column, table);
            // Extract variance, max, min, mean, mode values
            // https://www.postgresql.org/docs/9.1/functions-aggregate.html
            DateTimeColumnMetadata metadata;
            var numericStatQuery = String.Format(@"SELECT avg({0}) as average,
                            max({0}) as max,
                            min({0}) as min,
                            variance({0}) as variance
                            FROM {1}.{2}", 
                            column.name, table.schema, table.name);

            using (var cmd = new NpgsqlCommand(numericStatQuery, (NpgsqlConnection) connection))
            using (var reader = cmd.ExecuteReader())
            {
                metadata = new DateTimeColumnMetadata(defaultMetadata)
                {
                    max = reader.GetFieldValue<DateTime>("max"),
                    min = reader.GetFieldValue<DateTime>("min"),
                    variance = reader.GetFieldValue<DateTime>("variance"),
                    average = reader.GetFieldValue<DateTime>("average")
                };
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

        private TextColumnMetadata GetTextColumnMetadata(DbConnection connection, ColumnDescription column, TableDescription table)
        {
            ColumnMetadata defaultMetadata = GetDefaultColumnMetadata(connection, column, table);
            var metadata = new TextColumnMetadata(defaultMetadata);

            var countQuery = String.Format(@"SELECT count({0}) as count,
                FROM {1}.{2}", 
                column.name, table.schema, table.name);

            int rowCount;
            using (var cmd = new NpgsqlCommand(countQuery, (NpgsqlConnection) connection))
            {
                rowCount = (int) cmd.ExecuteScalar();
            }
            
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

            List<String> samples = new List<String>();

            using (var cmd = new NpgsqlCommand(dataSampleQuery, (NpgsqlConnection) connection))
            using (var reader = cmd.ExecuteReader())
            {
                foreach (String row in reader)
                {
                    samples.Add(row);
                }
            }

            metadata.entityType = Common.EntityMatcher.Match(samples);

            if (metadata.entityType == NamedEntity.Unknown)
            {
                // TODO - Handle unknown entity type, possibly try to convert to CategoricColumn
                throw new Exception("Could not identify textual data type");
            }

            return metadata;
        }

    }

}