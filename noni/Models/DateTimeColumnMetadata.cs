using System;

namespace noni.Models {

    // https://stackoverflow.com/questions/58074304/is-polymorphic-deserialization-possible-in-system-text-json
    public class DateTimeColumnMetadata : ColumnMetadata
    {
        public DateTime max{get;set;}
        public DateTime min{get;set;}
        public DateTime mode{get; set;}

        public DateTimeColumnMetadata(ColumnMetadata defaultMetadata)
        {
            this.distinct = defaultMetadata.distinct;
        }
    }

}