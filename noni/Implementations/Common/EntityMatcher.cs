using System;
using System.Collections.Generic;
using System.Linq;

using noni.Contracts;


namespace noni.Implementations.Common {

    public class EntityMatcher {

        public static NamedEntity Match(List<String> textData)
        {

            List<NamedEntity> matchedEntities = 
            textData.AsParallel().Select((x) => {
                return EntityMatcher.Match(x);
            }).ToList();

            var votedMatch = matchedEntities
                .GroupBy( n => n )
                .Select( n => new {
                    category = n.Key,
                    count = n.Count()
                })
                .OrderByDescending( n => n.count)
                .Select( n => n.category )
                .FirstOrDefault();

            return votedMatch;
        }

        public static NamedEntity Match(String text)
        {
            return NamedEntity.Unknown;
        }

    }

}