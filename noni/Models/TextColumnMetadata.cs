using System;
using noni.Contracts;

namespace noni.Models {

    // https://stackoverflow.com/questions/58074304/is-polymorphic-deserialization-possible-in-system-text-json
    public class TextColumnMetadata : ColumnMetadata
    {
        public String mode{get; set;}

        public int sampleCount{get; set;}
        public NamedEntity entityType = NamedEntity.Unknown;
        

        public TextColumnMetadata(ColumnMetadata defaultMetadata)
        {
            this.distinct = defaultMetadata.distinct;
        }
    }

}