using System;
using System.Data.Common;
using System.Data;
using System.Collections.Generic;

using noni.Models;

namespace noni.Contracts {

    public interface IDatabaseMetadataCollector
    {
        public DatabaseStructure Collect(DbConnection connection, DatabaseStructure originalStructure);
    }

}