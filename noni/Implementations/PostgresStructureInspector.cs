using System;
using System.Data.Common;
using System.Data;
using Npgsql;
using System.CommandLine.DragonFruit;
using System.Collections.Generic;

using noni.Contracts;
using noni.Models;

namespace noni.Implementations {

    /// <summary>
    /// Structure inspector for pg database
    /// </summary>
    public class PostgresStructureInspector : IStructureInspector {

        private const String DATABASE_STRUCTURE_DATA_TABLE_NAME = "all_columns";

        /// <summary>
        /// Extracts structure for a pg database
        /// </summary>
        public DatabaseStructure GetDatabaseStructure(DbConnection connection) 
        {
            
            // Query for schemas and tables in the database, except postgres default system tables
            String tablesColumnsQuery = @"SELECT t.table_schema, t.table_name, t.table_type, c.column_name, c.ordinal_position, 
                                            c.column_default, c.is_nullable, c.data_type
                                            FROM information_schema.tables as t
                                            inner join information_schema.columns as c 
                                            on t.table_name = c.table_name
                                            and t.table_catalog = c.table_catalog
                                            and t.table_schema = c.table_schema
                                            where t.table_schema not in ('pg_catalog', 'information_schema')
                                            and t.table_type = 'BASE TABLE';";

            var adapter = new NpgsqlDataAdapter(tablesColumnsQuery, (NpgsqlConnection) connection);
            DataSet resultDataset = new DataSet();
            adapter.Fill(resultDataset, DATABASE_STRUCTURE_DATA_TABLE_NAME);
            
            return ConvertDataTableToDatabaseStructure(resultDataset.Tables[DATABASE_STRUCTURE_DATA_TABLE_NAME]);

        }

        private DatabaseStructure ConvertDataTableToDatabaseStructure(DataTable dt)
        {
            DatabaseStructure structure = new DatabaseStructure();
            foreach (DataRow columnRow in dt.Rows)
            {

                var schema = (String) columnRow["table_schema"];
                var tableName = (String) columnRow["table_name"];
                var columnName = (String) columnRow["column_name"];
                var nativeType = (String) columnRow["data_type"];   
                
                Console.WriteLine("{0}.{1}.{2} [{3}]", columnRow["table_schema"], columnRow["table_name"], columnRow["column_name"], columnRow["data_type"]);
                
                TableDescription table = structure.GetExistingOrCreateTable(tableName, schema);
                ColumnDescription column = new ColumnDescription(columnName, nativeType, InferAgnosticType(nativeType, columnName));
                table.AddColumn(column);
            
            }
            return structure;
        }

        private AgnosticColumnType InferAgnosticType(string nativeType, string columnName) {

            // https://www.npgsql.org/doc/types/basic.html

            HashSet<string> timeTypes = new HashSet<String>{ "date", "timestamp without time zone", "timestamp with timezone", "time without time zone", "time with time zone" };
            HashSet<string> textTypes = new HashSet<String>{ "text", "character varying", "character", "citext", "json", "jsonb", "xml" };
            HashSet<string> integerTypes = new HashSet<String>{ "smallint", "integer", "bigint" };
            HashSet<string> decimalTypes = new HashSet<String>{ "real", "double precision", "numeric", "money" };
            HashSet<string> booleanTypes = new HashSet<String>{ "bit(1)", "bool" };

            if (columnName.Contains("_id") || columnName.StartsWith("id_") || nativeType.Equals("uuid") ) 
            {
                return AgnosticColumnType.Key;
            }

            if (timeTypes.Contains(nativeType)) return AgnosticColumnType.Time;
            if (textTypes.Contains(nativeType)) return AgnosticColumnType.Text;
            if (integerTypes.Contains(nativeType)) return AgnosticColumnType.Integer;
            if (decimalTypes.Contains(nativeType)) return AgnosticColumnType.Decimal;
            if (booleanTypes.Contains(nativeType)) return AgnosticColumnType.Bool;
                
            return AgnosticColumnType.Unknown;
        }

    }

}