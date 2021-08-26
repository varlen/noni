using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using noni.Serialization;

namespace noni.Models {

    /// <summary>
    /// Represents the database structure containing its tables, columns and relations
    /// </summary>
    public class DatabaseStructure {
        public Dictionary<Tuple<string,string>, TableDescription> tableDict
        {
            get;
            set;
        }
        
        /// <summary>
        /// Represents the database structure containing its tables, columns and relations
        /// </summary>
        public DatabaseStructure() {
            tableDict = new Dictionary<Tuple<string,string>, TableDescription>();
        }

        /// <summary>
        /// Returns enumerator for table dictionary
        /// </summary>
        public Dictionary<Tuple<string, string>, TableDescription> GetTables() {
            return tableDict;
        }
        
        /// <summary>
        /// Safely add table to database structure
        /// </summary>
        public void AddTable(TableDescription table) {

            var tableKey = GetKey(table.name, table.schema);
            if (tableDict.ContainsKey(tableKey)) {
                tableDict[tableKey] = table;
            } else {
                tableDict.Add(tableKey, table);
            }

        }

        private Tuple<string, string> GetKey(string name, string schema) {
            return new Tuple<string, string>(name, schema);
        }

        public bool HasTable(string name, string schema = "") {
            return tableDict.ContainsKey(GetKey(name,schema));
        }

        public TableDescription GetExistingOrCreateTable(string name, string schema = "") {
            TableDescription tableDescription;
            if (!tableDict.TryGetValue(GetKey(name, schema), out tableDescription)) 
            {
                tableDescription = new TableDescription(name, schema);
                tableDict.Add(GetKey(name, schema), tableDescription);
            }
            return tableDescription;
        }

        public override String ToString() {
            var tableList = tableDict.Values.ToList();
            var serializableDict = new Dictionary<string, List<TableDescription>>()
            {
                {"tables", tableList}
            };

            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                Converters = { 
                    new ColumnMetadataConverter(),
                    new JsonStringEnumConverter(JsonNamingPolicy.CamelCase) 
                }
            };

            return JsonSerializer.Serialize(serializableDict, options);
        }

        public static DatabaseStructure FromString(String serializedStructure) {
            throw new NotImplementedException();
        }


    }

}