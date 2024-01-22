using Quartz.Spi.MongoJobStore.Models.Id;

namespace Quartz.Spi.MongoJobStore.Models;

/*
 INSERT INTO
    {0}JOB_DETAILS (SCHED_NAME, JOB_NAME, JOB_GROUP, DESCRIPTION, JOB_CLASS_NAME, IS_DURABLE, IS_NONCONCURRENT, IS_UPDATE_DATA, REQUESTS_RECOVERY, JOB_DATA)
    VALUES(@schedulerName, @jobName, @jobGroup, @jobDescription, @jobType, @jobDurable, @jobVolatile, @jobStateful, @jobRequestsRecovery, @jobDataMap)
 */

internal class JobDetail
{
    public JobDetailId Id { get; set; }

    public string Description { get; set; }

    public Type JobType { get; set; }

    public JobDataMap JobDataMap { get; set; }

    public bool Durable { get; set; }

    public bool PersistJobDataAfterExecution { get; set; }

    public bool ConcurrentExecutionDisallowed { get; set; }

    public bool RequestsRecovery { get; set; }


    public JobDetail()
    {
    }

    public JobDetail(IJobDetail jobDetail, string instanceName)
    {
        Id = new JobDetailId(jobDetail.Key, instanceName);
        Description = jobDetail.Description;
        JobType = jobDetail.JobType;
        JobDataMap = jobDetail.JobDataMap;
        Durable = jobDetail.Durable;
        PersistJobDataAfterExecution = jobDetail.PersistJobDataAfterExecution;
        ConcurrentExecutionDisallowed = jobDetail.ConcurrentExecutionDisallowed;
        RequestsRecovery = jobDetail.RequestsRecovery;
    }

    public IJobDetail GetJobDetail()
    {
        // The missing properties are figured out at runtime from the job type attributes

        return JobBuilder.Create(JobType)
            .WithIdentity(new JobKey(Id.Name, Id.Group))
            .WithDescription(Description)
            .SetJobData(JobDataMap)
            .StoreDurably(Durable)
            .RequestRecovery(RequestsRecovery)
            .Build();
    }
}
