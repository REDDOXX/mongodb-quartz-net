using MongoDB.Driver;

using Quartz.Impl.Matchers;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

[CollectionName("jobs")]
internal class JobDetailRepository : BaseRepository<JobDetail>
{
    public JobDetailRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
                new CreateIndexModel<JobDetail>(
                    Builders<JobDetail>.IndexKeys.Combine(
                        Builders<JobDetail>.IndexKeys.Ascending(x => x.Id.InstanceName),
                        Builders<JobDetail>.IndexKeys.Ascending(x => x.Id.Name),
                        Builders<JobDetail>.IndexKeys.Ascending(x => x.Id.Group)
                    )
                )
            )
            .ConfigureAwait(false);
    }


    public async Task<JobDetail?> GetJob(JobKey jobKey)
    {
        return await Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName))
            .FirstOrDefaultAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<JobKey>> GetJobsKeys(GroupMatcher<JobKey> matcher)
    {
        var filter = FilterBuilder.And(
            FilterBuilder.Eq(detail => detail.Id.InstanceName, InstanceName),
            FilterBuilder.Regex(detail => detail.Id.Group, matcher.ToBsonRegularExpression())
        );

        return await Collection
            //
            .Find(filter)
            .Project(detail => detail.Id.GetJobKey())
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetJobGroupNames()
    {
        return await Collection.Distinct(detail => detail.Id.Group, detail => detail.Id.InstanceName == InstanceName)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task AddJob(JobDetail jobDetail)
    {
        await Collection.InsertOneAsync(jobDetail).ConfigureAwait(false);
    }

    public async Task<long> UpdateJob(JobDetail jobDetail, bool upsert)
    {
        var result = await Collection.ReplaceOneAsync(
                detail => detail.Id == jobDetail.Id,
                jobDetail,
                new ReplaceOptions
                {
                    IsUpsert = upsert,
                }
            )
            .ConfigureAwait(false);
        return result.ModifiedCount;
    }

    public async Task UpdateJobData(JobKey jobKey, JobDataMap jobDataMap)
    {
        var update = UpdateBuilder.Set(detail => detail.JobDataMap, jobDataMap);

        await Collection.UpdateOneAsync(detail => detail.Id == new JobDetailId(jobKey, InstanceName), update)
            .ConfigureAwait(false);
    }

    public async Task<long> DeleteJob(JobKey key)
    {
        var filter = FilterBuilder.Where(job => job.Id == new JobDetailId(key, InstanceName));

        var result = await Collection.DeleteOneAsync(filter).ConfigureAwait(false);
        return result.DeletedCount;
    }

    public async Task<bool> JobExists(JobKey jobKey)
    {
        return await Collection.Find(detail => detail.Id == new JobDetailId(jobKey, InstanceName))
            .AnyAsync()
            .ConfigureAwait(false);
    }

    public async Task<long> GetCount()
    {
        return await Collection.Find(detail => detail.Id.InstanceName == InstanceName)
            .CountDocumentsAsync()
            .ConfigureAwait(false);
    }
}
