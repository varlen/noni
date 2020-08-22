using noni.Contracts;
using System.Collections.Generic;

namespace noni.Models {
    /// <summary> Store database column caracteristics </summary>
    public class ColumnDescription {

        /// <summary>Column name</summary>
        public readonly string name;

        /// <summary>Agnostic column data type</summary>
        public readonly AgnosticColumnType type;

        /// <summary>Original column data type</summary>
        public readonly string nativeType;

        /// <summary>Parameters for data generation</summary>
        public Dictionary<string, string> generatorMetadata;

        /// <summary> Store database column description </summary>
        /// <param name="name">Column name</param>
        /// <param name="nativeType">Column type in the native database</param>
        /// <param name="type">Agnostic column data type</param>
        public ColumnDescription(string name, string nativeType, AgnosticColumnType type) {
            this.name = name;
            this.nativeType = nativeType;
            this.type = type;
        }
        
    }
}