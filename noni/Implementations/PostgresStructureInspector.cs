using System;
using System.Data.Common;
using System.CommandLine.DragonFruit;

using noni.Contracts;
using noni.Models;

namespace noni.Implementations {

    /// <summary>
    /// Structure inspector for pg database
    /// </summary>
    public class PostgresStructureInspector : IStructureInspector {

        /// <summary>
        /// Extracts structure for a pg database
        /// </summary>
        public DatabaseStructure GetDatabaseStructure(DbConnection connection) 
        {
            throw new NotImplementedException();
        }

    }

}