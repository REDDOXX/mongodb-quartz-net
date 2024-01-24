using MongoDB.Driver;

using Quartz.Impl.Matchers;
using Quartz.Spi.MongoJobStore.Extensions;
using Quartz.Spi.MongoJobStore.Models;
using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Repositories;

internal class JobDetailRepository : BaseRepository<JobDetail>
{
    public JobDetailRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "jobs", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        await Collection.Indexes.CreateOneAsync(
                new CreateIndexModel<JobDetail>(
                    Builders<JobDetail>.IndexKeys.Combine(
                        Builders<JobDetail>.IndexKeys.Ascending(x => x.InstanceName),
                        Builders<JobDetail>.IndexKeys.Ascending(x => x.Name),
                        Builders<JobDetail>.IndexKeys.Ascending(x => x.Group)
                    ),
                    new CreateIndexOptions
                    {
                        Unique = true,
                    }
                )
            )
            .ConfigureAwait(false);
    }


    public async Task<JobDetail?> GetJob(JobKey jobKey)
    {
        /*
            SELECT
                JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, REQUESTS_RECOVERY, JOB_DATA, IS_NONCONCURRENT, IS_UPDATE_DATA
            FROM
                JOB_DETAILS
            WHERE
                SCHED_NAME = @schedulerName AND
                JOB_NAME = @jobName AND
                JOB_GROUP = @jobGroup
        */

        try
        {
            var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                         FilterBuilder.Eq(x => x.Name, jobKey.Name) &
                         FilterBuilder.Eq(x => x.Group, jobKey.Group);

            return await Collection
                //
                .Find(filter)
                .FirstOrDefaultAsync()
                .ConfigureAwait(false);
        }
        catch (TypeLoadException ex)
        {
            throw new JobPersistenceException(
                $"Couldn't retrieve job because a required type was not found: {ex.Message}",
                ex
            );
        }
    }

    public async Task<List<JobKey>> GetJobsKeys(GroupMatcher<JobKey> matcher)
    {
        var filter2 = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                      FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression());


        return await Collection
            //
            .Find(filter2)
            .Project(x => new JobKey(x.Name, x.Group))
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task<List<string>> GetJobGroupNames()
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
            //
            .Distinct(detail => detail.Group, filter)
            .ToListAsync()
            .ConfigureAwait(false);
    }

    public async Task AddJob(JobDetail jobDetail)
    {
        await Collection.InsertOneAsync(jobDetail).ConfigureAwait(false);
    }

    public async Task<long> UpdateJob(JobDetail jobDetail, bool upsert)
    {
        var filter = FilterBuilder.Eq(x => x.InstanceName, jobDetail.InstanceName) &
                     FilterBuilder.Eq(x => x.Name, jobDetail.Name) &
                     FilterBuilder.Eq(x => x.Group, jobDetail.Group);

        var result = await Collection.ReplaceOneAsync(
                filter,
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
        //var filter = Builders<JobDetail>.Filter.Eq(x => x.Id.InstanceName, InstanceName) &
        //             Builders<JobDetail>.Filter.Eq(x => x.Id.Name, jobKey.Name) &
        //             Builders<JobDetail>.Filter.Eq(x => x.Id.Group, jobKey.Group);

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
