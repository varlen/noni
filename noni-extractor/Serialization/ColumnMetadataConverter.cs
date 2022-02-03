using System;
using System.Text.Json;
using System.Text.Json.Serialization;
using noni.Models;

namespace noni.Serialization {
    public class ColumnMetadataConverter : JsonConverter<ColumnMetadata>
    {
        private const string TYPE_DISCRIMINATOR_PROPERTY = "columnType";
        private const string TYPE_VALUE_PROPERTY = "columnData";

        // Column types
        private enum TypeDiscriminator {
            BaseClass = 0,
            Numeric = 1,
            Text = 2,
            DateTime = 3,
            Category = 4
        }

        public override bool CanConvert(Type typeToConvert)
        {
            return typeof(ColumnMetadata).IsAssignableFrom(typeToConvert);
        }
        public override void Write(Utf8JsonWriter writer, ColumnMetadata value, JsonSerializerOptions options)
        {
            writer.WriteStartObject();

            if (value is NumericColumnMetadata numericColumnMetadata) 
            {
                writer.WriteNumber(TYPE_DISCRIMINATOR_PROPERTY, (int) TypeDiscriminator.Numeric);
                writer.WritePropertyName(TYPE_VALUE_PROPERTY);
                JsonSerializer.Serialize(writer, numericColumnMetadata);
            }
            else if (value is DateTimeColumnMetadata dateTimeColumnMetadata)
            {
                writer.WriteNumber(TYPE_DISCRIMINATOR_PROPERTY, (int) TypeDiscriminator.DateTime);
                writer.WritePropertyName(TYPE_VALUE_PROPERTY);
                JsonSerializer.Serialize(writer, dateTimeColumnMetadata);
            }
            else if (value is TextColumnMetadata textColumnMetadata)
            {
                writer.WriteNumber(TYPE_DISCRIMINATOR_PROPERTY, (int) TypeDiscriminator.Text);
                writer.WritePropertyName(TYPE_VALUE_PROPERTY);
                JsonSerializer.Serialize(writer, textColumnMetadata);
            }
            else if (value is CategoricColumnMetadata categoricColumnMetadata)
            {
                writer.WriteNumber(TYPE_DISCRIMINATOR_PROPERTY, (int) TypeDiscriminator.Category);
                writer.WritePropertyName(TYPE_VALUE_PROPERTY);
                JsonSerializer.Serialize(writer, categoricColumnMetadata);
            }
            else if (value is ColumnMetadata columnMetadata)
            {
                writer.WriteNumber(TYPE_DISCRIMINATOR_PROPERTY, (int) TypeDiscriminator.BaseClass);
                writer.WritePropertyName(TYPE_VALUE_PROPERTY);
                JsonSerializer.Serialize(writer, columnMetadata);
            }
            else
            {
                Console.WriteLine($"Bad column serialization: {value}");
                throw new NotSupportedException();
            }

            writer.WriteEndObject();
        }

        public override ColumnMetadata Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
        {
            // Read the type discriminator value
            if (reader.TokenType != JsonTokenType.StartObject)
            {
                throw new JsonException();
            }

            if (!reader.Read()
                    || reader.TokenType != JsonTokenType.PropertyName
                    || reader.GetString() != TYPE_DISCRIMINATOR_PROPERTY)
            {
                throw new JsonException();
            }

            if (!reader.Read() || reader.TokenType != JsonTokenType.Number)
            {
                throw new JsonException();
            }

            ColumnMetadata columnMetadata;
            TypeDiscriminator typeDiscriminator = (TypeDiscriminator) reader.GetInt32();
            switch (typeDiscriminator)
            {
                case TypeDiscriminator.Category:
                    if (!reader.Read() || reader.GetString() != TYPE_VALUE_PROPERTY)
                    {
                        throw new JsonException();
                    }
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    {
                        throw new JsonException();
                    }
                    columnMetadata = (CategoricColumnMetadata) JsonSerializer.Deserialize(ref reader, typeof(CategoricColumnMetadata));
                    break;
                case TypeDiscriminator.Numeric:
                    if (!reader.Read() || reader.GetString() != TYPE_VALUE_PROPERTY)
                    {
                        throw new JsonException();
                    }
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    {
                        throw new JsonException();
                    }
                    columnMetadata = (NumericColumnMetadata) JsonSerializer.Deserialize(ref reader, typeof(NumericColumnMetadata));
                    break;
                case TypeDiscriminator.Text:
                    if (!reader.Read() || reader.GetString() != TYPE_VALUE_PROPERTY)
                    {
                        throw new JsonException();
                    }
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    {
                        throw new JsonException();
                    }
                    columnMetadata = (TextColumnMetadata) JsonSerializer.Deserialize(ref reader, typeof(TextColumnMetadata));
                    break;
                case TypeDiscriminator.DateTime:
                    if (!reader.Read() || reader.GetString() != TYPE_VALUE_PROPERTY)
                    {
                        throw new JsonException();
                    }
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    {
                        throw new JsonException();
                    }
                    columnMetadata = (DateTimeColumnMetadata) JsonSerializer.Deserialize(ref reader, typeof(DateTimeColumnMetadata));
                    break;
                case TypeDiscriminator.BaseClass:
                    if (!reader.Read() || reader.GetString() != TYPE_VALUE_PROPERTY)
                    {
                        throw new JsonException();
                    }
                    if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
                    {
                        throw new JsonException();
                    }
                    columnMetadata = (ColumnMetadata) JsonSerializer.Deserialize(ref reader, typeof(ColumnMetadata));
                    break;
                default:
                    Console.WriteLine($"Bad column metadata deserialization: {typeDiscriminator}");
                    throw new NotSupportedException();
            }

            if (!reader.Read() || reader.TokenType != JsonTokenType.EndObject)
            {
                throw new JsonException();
            }

            return columnMetadata;
        }


    }
}