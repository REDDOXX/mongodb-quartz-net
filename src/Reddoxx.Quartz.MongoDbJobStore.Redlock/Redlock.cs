#region LICENSE

/*
 *   Copyright 2014 Angelo Simone Scotto <scotto.a@gmail.com>
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 * */

#endregion


using System.Diagnostics.CodeAnalysis;
using System.Text;

using StackExchange.Redis;

namespace Redlock.CSharp;

internal class Redlock
{
    public Redlock(params IConnectionMultiplexer[] list)
    {
        foreach (var item in list)
        {
            _redisMasterDictionary.Add(item.GetEndPoints().First().ToString()!, item);
        }
    }

    private const int DefaultRetryCount = 3;
    private readonly TimeSpan _defaultRetryDelay = new TimeSpan(0, 0, 0, 0, 200);
    private const double ClockDriveFactor = 0.01;

    protected int Quorum => (_redisMasterDictionary.Count / 2) + 1;

    /// <summary>
    /// String containing the Lua unlock script.
    /// </summary>
    private const string UnlockScript = """
                                          if redis.call("get",KEYS[1]) == ARGV[1] then
                                              return redis.call("del",KEYS[1])
                                          else
                                              return 0
                                          end
                                        """;


    protected static byte[] CreateUniqueLockId()
    {
        return Guid.NewGuid().ToByteArray();
    }


    protected readonly Dictionary<string, IConnectionMultiplexer> _redisMasterDictionary = [];

    //TODO: Refactor passing a ConnectionMultiplexer
    protected bool LockInstance(string redisServer, RedisKey resource, byte[] val, TimeSpan ttl)
    {
        bool succeeded;
        try
        {
            var redis = _redisMasterDictionary[redisServer];
            succeeded = redis.GetDatabase().StringSet(resource, val, ttl, When.NotExists);
        }
        catch (Exception)
        {
            succeeded = false;
        }

        return succeeded;
    }

    //TODO: Refactor passing a ConnectionMultiplexer
    protected void UnlockInstance(string redisServer, RedisKey resource, RedisValue val)
    {
        RedisKey[] key = [resource,];
        RedisValue[] values = [val,];

        var redis = _redisMasterDictionary[redisServer];
        redis.GetDatabase().ScriptEvaluate(UnlockScript, key, values);
    }

    public bool Lock(RedisKey resource, TimeSpan ttl, [MaybeNullWhen(false)] out Lock lockObject)
    {
        var val = CreateUniqueLockId();
        Lock? innerLock = null;
        var successful = Retry(
            DefaultRetryCount,
            _defaultRetryDelay,
            () =>
            {
                try
                {
                    var n = 0;
                    var startTime = DateTime.Now;

                    // Use keys
                    for_each_redis_registered(
                        redis =>
                        {
                            if (LockInstance(redis, resource, val, ttl))
                            {
                                n += 1;
                            }
                        }
                    );

                    /*
                     * Add 2 milliseconds to the drift to account for Redis expires
                     * precision, which is 1 millisecond, plus 1 millisecond min drift
                     * for small TTLs.
                     */
                    var drift = (int)(ttl.TotalMilliseconds * ClockDriveFactor + 2);
                    var validityTime = ttl - (DateTime.Now - startTime) - new TimeSpan(0, 0, 0, 0, drift);

                    if (n >= Quorum && validityTime.TotalMilliseconds > 0)
                    {
                        innerLock = new Lock(resource, val, validityTime);
                        return true;
                    }

                    for_each_redis_registered(redis => { UnlockInstance(redis, resource, val); });
                    return false;
                }
                catch (Exception)
                {
                    return false;
                }
            }
        );

        lockObject = innerLock;
        return successful;
    }

    protected void for_each_redis_registered(Action<IConnectionMultiplexer> action)
    {
        foreach (var item in _redisMasterDictionary)
        {
            action(item.Value);
        }
    }

    private void for_each_redis_registered(Action<string> action)
    {
        foreach (var item in _redisMasterDictionary)
        {
            action(item.Key);
        }
    }

    private static bool Retry(int retryCount, TimeSpan retryDelay, Func<bool> action)
    {
        var maxRetryDelay = (int)retryDelay.TotalMilliseconds;
        var rnd = new Random();
        var currentRetry = 0;

        while (currentRetry++ < retryCount)
        {
            if (action())
            {
                return true;
            }

            Thread.Sleep(rnd.Next(maxRetryDelay));
        }

        return false;
    }

    public void Unlock(Lock lockObject)
    {
        for_each_redis_registered(redis => { UnlockInstance(redis, lockObject.Resource, lockObject.Value); });
    }

    public override string ToString()
    {
        var sb = new StringBuilder();
        sb.AppendLine(GetType().FullName);

        sb.AppendLine("Registered Connections:");
        foreach (var item in _redisMasterDictionary)
        {
            sb.AppendLine(item.Value.GetEndPoints().First().ToString());
        }

        return sb.ToString();
    }
}
