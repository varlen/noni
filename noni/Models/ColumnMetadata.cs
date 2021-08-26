using System;
namespace noni.Models {

    // TODO - Fix metadata serialization
    // https://stackoverflow.com/questions/58074304/is-polymorphic-deserialization-possible-in-system-text-json    
    // https://stackoverflow.com/questions/58074304/is-polymorphic-deserialization-possible-in-system-text-json
    // https://stackoverflow.com/questions/29528648/json-net-serialization-of-type-with-polymorphic-child-object
    // https://github.com/manuc66/JsonSubTypes
    public class ColumnMetadata
    {
        public Int64 distinct{get;set;}
    }

}