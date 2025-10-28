using MongoDB.Driver;

using Quartz;
using Quartz.Impl.Matchers;

using Reddoxx.Quartz.MongoDbJobStore.Extensions;
using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore.Repositories;

internal class JobDetailRepository : BaseRepository<JobDetail>
{
    public JobDetailRepository(IMongoDatabase database, string instanceName, string? collectionPrefix = null)
        : base(database, "jobs", instanceName, collectionPrefix)
    {
    }

    public override async Task EnsureIndex()
    {
        // PK_QRTZ_JOB_DETAILS
        await Collection.Indexes.CreateOneAsync(
            new CreateIndexModel<JobDetail>(
                IndexBuilder.Ascending(x => x.InstanceName)
                            .Ascending(x => x.Name)
                            .Ascending(x => x.Group),
                new CreateIndexOptions
                {
                    Unique = true,
                }
            )
        );
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
                         .FirstOrDefaultAsync();
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
        // SELECT JOB_NAME, JOB_GROUP FROM JOB_DETAILS WHERE SCHED_NAME = @schedulerName AND JOB_GROUP = @jobGroup
        // SELECT JOB_NAME, JOB_GROUP FROM JOB_DETAILS WHERE SCHED_NAME = @schedulerName AND JOB_GROUP LIKE @jobGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Regex(x => x.Group, matcher.ToBsonRegularExpression());

        return await Collection
                     //
                     .Find(filter)
                     .Project(x => new JobKey(x.Name, x.Group))
                     .ToListAsync();
    }

    public async Task<List<string>> GetJobGroupNames()
    {
        // SELECT DISTINCT(JOB_GROUP) FROM JOB_DETAILS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
                     //
                     .Distinct(detail => detail.Group, filter)
                     .ToListAsync();
    }

    public async Task AddJob(JobDetail jobDetail)
    {
        await Collection.InsertOneAsync(jobDetail);
    }

    public async Task<long> UpdateJob(JobDetail jobDetail)
    {
        // UPDATE JOB_DETAILS SET
        //  DESCRIPTION = @jobDescription,
        //  JOB_CLASS_NAME = @jobType,
        //  IS_DURABLE = @jobDurable,
        //  IS_NONCONCURRENT = @jobVolatile,
        //  IS_UPDATE_DATA = @jobStateful,
        //  REQUESTS_RECOVERY = @jobRequestsRecovery,
        //  JOB_DATA = @jobDataMap
        // WHERE
        //  SCHED_NAME = @schedulerName AND
        //  JOB_NAME = @jobName AND
        //  JOB_GROUP = @jobGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, jobDetail.InstanceName) &
                     FilterBuilder.Eq(x => x.Name, jobDetail.Name) &
                     FilterBuilder.Eq(x => x.Group, jobDetail.Group);

        var update = UpdateBuilder
                     //
                     .Set(x => x.Description, jobDetail.Description)
                     .Set(x => x.JobType, jobDetail.JobType)
                     .Set(x => x.Durable, jobDetail.Durable)
                     .Set(x => x.ConcurrentExecutionDisallowed, jobDetail.ConcurrentExecutionDisallowed)
                     .Set(x => x.PersistJobDataAfterExecution, jobDetail.PersistJobDataAfterExecution)
                     .Set(x => x.RequestsRecovery, jobDetail.RequestsRecovery)
                     .Set(x => x.JobDataMap, jobDetail.JobDataMap);

        var result = await Collection.UpdateOneAsync(filter, update);
        return result.ModifiedCount;
    }

    public async Task UpdateJobData(JobKey jobKey, JobDataMap jobDataMap)
    {
        // UPDATE JOB_DETAILS SET JOB_DATA = @jobDataMap WHERE SCHED_NAME = @schedulerName AND JOB_NAME = @jobName AND JOB_GROUP = @jobGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.Group, jobKey.Group);

        var update = UpdateBuilder.Set(detail => detail.JobDataMap, jobDataMap);

        await Collection.UpdateOneAsync(filter, update);
    }


    public async Task<long> DeleteJob(JobKey key)
    {
        // DELETE FROM JOB_DETAILS WHERE SCHED_NAME = @schedulerName AND JOB_NAME = @jobName AND JOB_GROUP = @jobGroup
        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, key.Name) &
                     FilterBuilder.Eq(x => x.Group, key.Group);

        var result = await Collection.DeleteOneAsync(filter);
        return result.DeletedCount;
    }

    public async Task<bool> JobExists(JobKey jobKey)
    {
        // SELECT 1 FROM JOB_DETAILS WHERE SCHED_NAME = @schedulerName AND JOB_NAME = @jobName AND JOB_GROUP = @jobGroup

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName) &
                     FilterBuilder.Eq(x => x.Name, jobKey.Name) &
                     FilterBuilder.Eq(x => x.Group, jobKey.Group);

        return await Collection.Find(filter)
                               .AnyAsync();
    }

    public async Task<long> GetCount()
    {
        // SELECT COUNT(JOB_NAME)  FROM JOB_DETAILS WHERE SCHED_NAME = @schedulerName

        var filter = FilterBuilder.Eq(x => x.InstanceName, InstanceName);

        return await Collection
                     //
                     .Find(filter)
                     .CountDocumentsAsync();
    }
}
