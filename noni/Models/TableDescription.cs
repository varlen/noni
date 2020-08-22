using noni.Contracts;
using System.Collections.Generic;

namespace noni.Models {
    /// <summary> Store database table caracteristics </summary>
    public class TableDescription {

        /// <summary> Table name </summary>
        public readonly string Name;

        /// <summary> Table schema name. Optional. </summary>
        public readonly string Schema;
        
        private List<ColumnDescription> columns;

        /// <summary> Store database table caracteristics </summary>
        /// <param name="name"> Table name</param>
        /// <param name="schema"> Optional table schema</param>
        public TableDescription(string name, string schema = "") {
            Name = name;
            Schema = schema;
            columns = new List<ColumnDescription>();
        }

        /// <summary> Columns collection </summary>
        public List<ColumnDescription> GetColumns() {
            return columns;
        }
        
        /// <summary> Add a new column description to table description </summary>
        public void AddColumn(ColumnDescription column) {
            columns.Add(column);
        }
    }
}