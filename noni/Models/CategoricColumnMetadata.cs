using System;
using System.Collections.Generic;
using noni.Contracts;

namespace noni.Models {


    public class CategoricColumnMetadata : ColumnMetadata
    {
        public NamedEntity entityType = NamedEntity.Unknown;
        public Dictionary<Object, int> categories = new Dictionary<Object, int>();

        public new decimal distinct{ 
            get {
                return categories.Count;
            }
        }

    }

}