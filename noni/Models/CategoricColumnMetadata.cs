using System;
using System.Collections.Generic;
using noni.Contracts;

namespace noni.Models {


    public class CategoricColumnMetadata : ColumnMetadata
    {
        public NamedEntity entityType = NamedEntity.Unknown;
        public Dictionary<Object, Int64> categories;

        public CategoricColumnMetadata()
        {
            categories = new Dictionary<Object, Int64>();
        }

        public static Dictionary<Object, Int64> TryCategoriesFromSamples(IEnumerable<string> samples)
        {
            var categories = new Dictionary<Object, Int64>();
            foreach (var item in samples)
            {
                if (categories.ContainsKey(item)) 
                {
                    categories[item] += 1;
                }
                else
                {
                    categories.Add(item, 1);
                }
            }

            return categories;
        }

        public CategoricColumnMetadata(Dictionary<Object, Int64> externalCategories)
        {
            categories = externalCategories;
        }

        public new decimal distinct{ 
            get {
                return categories.Count;
            }
        }

    }

}