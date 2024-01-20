using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

using Quartz.Simpl;

namespace Quartz.Spi.MongoJobStore.Serializers;

internal class JobDataMapSerializer : SerializerBase<JobDataMap>
{
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobDataMap value)
    {
        if (value == null)
        {
            context.Writer.WriteNull();
            return;
        }

        var base64 = Convert.ToBase64String(MongoDbJobStore.ObjectSerializer.Serialize(value));
        context.Writer.WriteString(base64);
    }

    public override JobDataMap Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        if (context.Reader.CurrentBsonType == BsonType.Null)
        {
            context.Reader.ReadNull();
            return null;
        }

        var bytes = Convert.FromBase64String(context.Reader.ReadString());
        return MongoDbJobStore.ObjectSerializer.DeSerialize<JobDataMap>(bytes);
    }
}
