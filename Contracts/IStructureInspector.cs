using System;
using noni.Models;
using System.Data.Common;

namespace noni.Contracts {

    /// <summary>
    /// Provides methods to retrieve database structure
    /// </summary>
    public interface IStructureInspector
    {

        /// <summary>
        /// Inspects the database structure to retrive tables, columns and relations
        /// </summary>
        public DatabaseStructure GetDatabaseStructure(DbConnection connection)
        
    }

}