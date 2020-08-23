using noni.Contracts;
using System.Collections.Generic;
using System.Text.Json;
using System;

namespace noni.Models {
    /// <summary> Store database table caracteristics </summary>
    public class TableDescription {

        /// <summary> Table name </summary>
        public String name {get; set;}
        /// <summary> Table schema name. Optional. </summary>
        public String schema {get; set;}
        private List<ColumnDescription> _columns;

        public List<ColumnDescription> columns{
            get { return _columns; }
        }

        /// <summary> Store database table caracteristics </summary>
        /// <param name="name"> Table name</param>
        /// <param name="schema"> Optional table schema</param>
        public TableDescription(String name, String schema = "") {
            this.name = name;
            this.schema = schema;
            _columns = new List<ColumnDescription>();
        }

        /// <summary> Columns collection </summary>
        public List<ColumnDescription> GetColumns() {
            return _columns;
        }
        
        /// <summary> Add a new column description to table description </summary>
        public void AddColumn(ColumnDescription column) {
            _columns.Add(column);
        }

        public string Serialize() {
            var options = new JsonSerializerOptions
            {
                IgnoreReadOnlyProperties = false,
                PropertyNameCaseInsensitive = false,
                IgnoreNullValues = false,
                MaxDepth = 1000,
                WriteIndented = true
            };

            return JsonSerializer.Serialize(this._columns, options);
            //return String.Format("{0} - {1}", Name, Schema);
        }
    }
}