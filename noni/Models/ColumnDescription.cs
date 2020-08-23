using noni.Contracts;
using System.Collections.Generic;

namespace noni.Models {
    /// <summary> Store database column caracteristics </summary>
    public class ColumnDescription {

        /// <summary>Column name</summary>
        public string name {get; set;}

        /// <summary>Agnostic column data type</summary>
        public AgnosticColumnType type {get; set;}

        /// <summary>Original column data type</summary>
        public string nativeType {get; set;}

        /// <summary>Parameters for data generation</summary>
        private Dictionary<string, string> _generatorMetadata = new Dictionary<string, string>();
        public Dictionary<string, string> generatorMetadata{ 
            get { return _generatorMetadata; }
        }
    }
}