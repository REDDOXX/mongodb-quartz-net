using Quartz.Simpl;

namespace Quartz.Spi.MongoJobStore.Tests;

/// <summary>
/// Generic extension methods for objects.
/// </summary>
public static class ObjectExtensions
{
    /// <summary>
    /// Creates a deep copy of object by serializing to memory stream.
    /// </summary>
    /// <param name="obj"></param>
    public static T? DeepClone<T>(this T? obj)
        where T : class
    {
        if (obj == null)
        {
            return null;
        }

        var bf = new JsonObjectSerializer();

        var buffer = bf.Serialize(obj);
        return bf.DeSerialize<T>(buffer);
    }
}
