using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

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
            switch(true)
            {
                case bool _ when Regex.IsMatch(text, @"(^\d{10,11}$)|(^\d{3}\.\d{3}\.\d{3}-\d{2}$)"):
                    return NamedEntity.CPF;

                case bool _ when Regex.IsMatch(text, @"^(\d{8}|\d{2}\.?\d{3}\-\d{3})$"):
                    return NamedEntity.CEP;

                case bool _ when Regex.IsMatch(text, @"^(\d{4} ?){4}$"):
                    return NamedEntity.CreditCardNumber;
                
                case bool _ when Regex.IsMatch(text, @"^(\d{14}|\d{2}\.?\d{3}\.?\d{3}\/?\d{4}\-?\d{2})$"):
                    return NamedEntity.CNPJ;

                case bool _ when Regex.IsMatch(text, @"(^([a-z]|[A-Z]){3}-?\d{4}$)|(^([a-z]|[A-Z]){3}\d([a-z]|[A-Z])\d{2}$)"):
                    return NamedEntity.LicensePlate;

                case bool _ when Regex.IsMatch(text, @"^\d{2}.?\d{3}.?\d{3}-?\d$"):
                    return NamedEntity.ID;

                case bool _ when Regex.IsMatch(text, @"^(\w+ ?)+$"):
                    return NamedEntity.Name;

                case bool _ when Regex.IsMatch(text, @"^[a-z0-9.]+@[a-z0-9]+\.[a-z]+\.([a-z]+)?$"):
                    return NamedEntity.Email;

                case bool _ when Regex.IsMatch(text, @"^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$"):
                    return NamedEntity.UUID;

                case bool _ when Regex.IsMatch(text, @"^\s*(?:\+?(\d{1,3}))?[-. (]*(\d{3})[-. )]*(\d{3})[-. ]*(\d{4})(?: *x(\d+))?\s*$"):
                    return NamedEntity.PhoneNumber;

                case bool _ when Regex.IsMatch(text, @"^\d{11}$"):
                    return NamedEntity.PIS;

                case bool _ when Regex.IsMatch(text, @"^(\w+ ?)+.+\d+$"):
                    return NamedEntity.Address;

                default:
                    return NamedEntity.Unknown;
            }

            return NamedEntity.Unknown;
        }

    }

}