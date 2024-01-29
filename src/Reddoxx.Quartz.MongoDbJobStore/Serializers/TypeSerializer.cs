using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

using Quartz.Util;

namespace Reddoxx.Quartz.MongoDbJobStore.Serializers;

internal class TypeSerializer : SerializerBase<Type>
{
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Type value)
    {
        var typeName = value.AssemblyQualifiedNameWithoutVersion();

        context.Writer.WriteString(typeName);
    }

    public override Type Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        var value = context.Reader.ReadString();
        var type = Type.GetType(value);

        if (type == null)
        {
            throw new TypeLoadException($"Could not load type '{value}'");
        }

        return type;
    }
}
