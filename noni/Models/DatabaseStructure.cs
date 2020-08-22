using System;
using System.Collections.Generic;

namespace noni.Models {

    /// <summary>
    /// Represents the database structure containing its tables, columns and relations
    /// </summary>
    public class DatabaseStructure {
        private Dictionary<Tuple<string,string>, TableDescription> tableDict;
        
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

            var tableKey = GetKey(table.Name, table.Schema);
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

        public String ToString() {
            throw new NotImplementedException();
        }

        public static DatabaseStructure FromString(String serializedStructure) {
            throw new NotImplementedException();
        }


    }

}