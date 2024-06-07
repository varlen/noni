
It is possible to replace SATO with any API that receives csv files and returns a JSON list of the semantic types of the columns, as long as the types are constrained to the [type78 list of types](https://raw.githubusercontent.com/varlen/sato/master/configs/types.json).

This opens the possibility of using an LLM for this purpose. I've made some tests on using a LLM for this purpose but haven't gone further than that.

LLM prompt example:
```
I want you to provide semantic types for CSV data.  I'll give you the CSV data starting with the column names. Please provide a valid semantic type for each column, separated by comma. Constrain the semantic types to the ones present in the following JSON list:
["address", "affiliate", "affiliation", "age", "album", "area", "artist", "birthDate", "birthPlace", "brand", "capacity", "category", "city", "class", "classification", "club", "code", "collection", "command", "company", "component", "continent", "country", "county", "creator", "credit", "currency", "day", "depth", "description", "director", "duration", "education", "elevation", "family", "fileSize", "format", "gender", "genre", "grades", "isbn", "industry", "jockey", "language", "location", "manufacturer", "name", "nationality", "notes", "operator", "order", "organisation", "origin", "owner", "person", "plays", "position", "product", "publisher", "range", "rank", "ranking", "region", "religion", "requirement", "result", "sales", "service", "sex", "species", "state", "status", "symbol", "team", "teamName", "type", "weight", "year"]
Give your answer formatted as a JSON array, like the example:
[{
   "column" : "<the name of the column>",
   "type" : "<the semantic type for the column>",
   "reasoning" : "<a simple explanation for why the type was chosen>"
}]
```
