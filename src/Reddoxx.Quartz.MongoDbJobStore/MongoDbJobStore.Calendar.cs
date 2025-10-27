using Quartz;

using Reddoxx.Quartz.MongoDbJobStore.Models;

namespace Reddoxx.Quartz.MongoDbJobStore;

public partial class MongoDbJobStore
{
    public async Task<bool> CalendarExists(string calName, CancellationToken token = default)
    {
        return await _calendarRepository.CalendarExists(calName).ConfigureAwait(false);
    }

    public Task StoreCalendar(
        string name,
        ICalendar calendar,
        bool replaceExisting,
        bool updateTriggers,
        CancellationToken cancellationToken = default
    )
    {
        try
        {
            return ExecuteInTx(
                QuartzLockType.TriggerAccess,
                () => StoreCalendarInternal(name, calendar, replaceExisting, updateTriggers, cancellationToken),
                cancellationToken
            );
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public Task<bool> RemoveCalendar(string calName, CancellationToken token = default)
    {
        try
        {
            return ExecuteInTx(QuartzLockType.TriggerAccess, () => RemoveCalendarInternal(calName), token);
        }
        catch (Exception ex)
        {
            throw new JobPersistenceException(ex.Message, ex);
        }
    }

    public async Task<ICalendar?> RetrieveCalendar(string calName, CancellationToken token = default)
    {
        return await _calendarRepository.GetCalendar(calName).ConfigureAwait(false);
    }

    public async Task<int> GetNumberOfCalendars(CancellationToken token = default)
    {
        return (int)await _calendarRepository.GetCount().ConfigureAwait(false);
    }

    public async Task<IReadOnlyCollection<string>> GetCalendarNames(CancellationToken token = default)
    {
        return await _calendarRepository.GetCalendarNames().ConfigureAwait(false);
    }


    private async Task StoreCalendarInternal(
        string calName,
        ICalendar calendar,
        bool replaceExisting,
        bool updateTriggers,
        CancellationToken token = default
    )
    {
        var existingCal = await CalendarExists(calName, token).ConfigureAwait(false);
        if (existingCal && !replaceExisting)
        {
            throw new ObjectAlreadyExistsException($"Calendar with name '{calName}' already exists.");
        }

        var persistentCalendar = new Calendar(calName, calendar, InstanceName);

        if (existingCal)
        {
            if (await _calendarRepository.UpdateCalendar(persistentCalendar).ConfigureAwait(false) < 1)
            {
                throw new JobPersistenceException("Couldn't store calendar.  Update failed.");
            }

            if (updateTriggers)
            {
                var triggers = await _triggerRepository.SelectTriggersForCalendar(calName).ConfigureAwait(false);

                foreach (var trigger in triggers)
                {
                    var quartzTrigger = trigger.GetTrigger();

                    quartzTrigger.UpdateWithNewCalendar(calendar, MisfireThreshold);

                    await StoreTriggerInternal(quartzTrigger, null, true, Models.TriggerState.Waiting, false, false)
                        .ConfigureAwait(false);
                }
            }
        }
        else
        {
            await _calendarRepository.AddCalendar(persistentCalendar).ConfigureAwait(false);
        }
    }

    private async Task<bool> RemoveCalendarInternal(string calendarName)
    {
        if (await _triggerRepository.CalendarIsReferenced(calendarName).ConfigureAwait(false))
        {
            throw new JobPersistenceException("Calender cannot be removed if it referenced by a trigger!");
        }

        return await _calendarRepository.DeleteCalendar(calendarName).ConfigureAwait(false) > 0;
    }
}
