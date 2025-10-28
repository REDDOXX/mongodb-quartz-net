using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Options;
using MongoDB.Bson.Serialization.Serializers;

using Quartz;

namespace Reddoxx.Quartz.MongoDbJobStore.Serializers;

internal class JobDataMapSerializer : SerializerBase<JobDataMap>
{
    private readonly DictionaryInterfaceImplementerSerializer<Dictionary<string, object>> _serializer =
        new(DictionaryRepresentation.Document);


    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, JobDataMap value)
    {
        if (value.WrappedMap is not Dictionary<string, object> map)
        {
            throw new InvalidOperationException("Wrapped map has an invalid type");
        }

        _serializer.Serialize(context, args, map);
    }

    public override JobDataMap Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        IDictionary<string, object> map = _serializer.Deserialize(context, args);

        return new JobDataMap(map);
    }
}
