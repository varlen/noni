using System;

namespace noni.Models {

    // https://stackoverflow.com/questions/58074304/is-polymorphic-deserialization-possible-in-system-text-json
    public class NumericColumnMetadata : ColumnMetadata
    {
        public Decimal variance{get;set;}
        public Decimal max{get;set;}
        public Decimal min{get;set;}
        public Decimal average{get;set;}
        public Decimal mode{get; set;}

        public NumericColumnMetadata(ColumnMetadata defaultMetadata)
        {
            this.distinct = defaultMetadata.distinct;
        }
    }

}