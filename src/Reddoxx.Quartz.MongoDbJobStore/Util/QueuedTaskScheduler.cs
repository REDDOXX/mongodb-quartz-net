//--------------------------------------------------------------------------
//
//  Copyright (c) Microsoft Corporation.  All rights reserved.
//
//  File: QueuedTaskScheduler.cs
//
//--------------------------------------------------------------------------

using System.Collections.Concurrent;
using System.Diagnostics;


namespace Reddoxx.Quartz.MongoDbJobStore.Util;

// Taken from: https://github.com/quartznet/quartznet/blob/main/src/Quartz/Util/QueuedTaskScheduler.cs

/// <summary>
/// Provides a TaskScheduler that provides control over the underlying threads utilized.
/// </summary>
[DebuggerTypeProxy(typeof(QueuedTaskSchedulerDebugView))]
[DebuggerDisplay("Id={Id}, ScheduledTasks = {DebugTaskCount}")]
internal sealed class QueuedTaskScheduler : TaskScheduler, IDisposable
{
    /// <summary>Debug view for the QueuedTaskScheduler.</summary>
    private sealed class QueuedTaskSchedulerDebugView
    {
        /// <summary>The scheduler.</summary>
        private readonly QueuedTaskScheduler _scheduler;

        /// <summary>Initializes the debug view.</summary>
        /// <param name="scheduler">The scheduler.</param>
        public QueuedTaskSchedulerDebugView(QueuedTaskScheduler scheduler)
        {
            _scheduler = scheduler ?? throw new ArgumentNullException(nameof(scheduler));
        }

        /// <summary>Gets all of the Tasks queued to the scheduler directly.</summary>
        public IEnumerable<Task> ScheduledTasks => _scheduler._blockingTaskQueue.ToList();
    }

    /// <summary>Cancellation token used for disposal.</summary>
    private readonly CancellationTokenSource _disposeCancellation = new();

    /// <summary>Whether we're processing tasks on the current thread.</summary>
    private static readonly ThreadLocal<bool> TaskProcessingThread = new();

    /// <summary>The threads used by the scheduler to process work.</summary>
    private readonly Thread[] _threads;

    /// <summary>The collection of tasks to be executed on our custom threads.</summary>
    private readonly BlockingCollection<Task> _blockingTaskQueue = [];

    /// <summary>Initializes the scheduler.</summary>
    /// <param name="threadCount">The number of threads to create and use for processing work items.</param>
    public QueuedTaskScheduler(int threadCount)
        : this(threadCount, string.Empty)
    {
    }

    /// <summary>Initializes the scheduler.</summary>
    /// <param name="threadCount">The number of threads to create and use for processing work items.</param>
    /// <param name="threadName">The name to use for each of the created threads.</param>
    /// <param name="useForegroundThreads">A Boolean value that indicates whether to use foreground threads instead of background.</param>
    /// <param name="threadPriority">The priority to assign to each thread.</param>
    public QueuedTaskScheduler(
        int threadCount,
        string threadName = "",
        bool useForegroundThreads = false,
        ThreadPriority threadPriority = ThreadPriority.Normal
    )
    {
        // Validates arguments (some validation is left up to the Thread type itself).
        // If the thread count is 0, default to the number of logical processors.
        if (threadCount < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(threadCount));
        }

        if (threadCount == 0)
        {
            MaximumConcurrencyLevel = Environment.ProcessorCount;
        }
        else
        {
            MaximumConcurrencyLevel = threadCount;
        }

        // Initialize the queue used for storing tasks

        // Create all of the threads
        _threads = new Thread[threadCount];
        for (var i = 0; i < threadCount; i++)
        {
            _threads[i] = new Thread(ThreadBasedDispatchLoop)
            {
                Priority = threadPriority,
                IsBackground = !useForegroundThreads,
            };

            if (!string.IsNullOrEmpty(threadName))
            {
                _threads[i].Name = $"{threadName} ({i})";
            }
        }

        // Start all of the threads
        foreach (var thread in _threads)
        {
            thread.Start();
        }
    }

    /// <summary>The dispatch loop run by all threads in this scheduler.</summary>
    private void ThreadBasedDispatchLoop()
    {
        TaskProcessingThread.Value = true;
        try
        {
            // If a thread abort occurs, we'll try to reset it and continue running.
            while (true)
            {
                try
                {
                    // For each task queued to the scheduler, try to execute it.
                    foreach (var task in _blockingTaskQueue.GetConsumingEnumerable(_disposeCancellation.Token))
                    {
                        TryExecuteTask(task);
                    }
                }
                catch (ThreadAbortException)
                {
                    // If we received a thread abort, and that thread abort was due to shutting down
                    // or unloading, let it pass through.  Otherwise, reset the abort so we can
                    // continue processing work items.
                    if (!Environment.HasShutdownStarted && !AppDomain.CurrentDomain.IsFinalizingForUnload())
                    {
#pragma warning disable SYSLIB0006
                        Thread.ResetAbort();
#pragma warning restore SYSLIB0006
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            // If the scheduler is disposed, the cancellation token will be set and
            // we'll receive an OperationCanceledException.  That OCE should not crash the process.
        }
        finally
        {
            TaskProcessingThread.Value = false;
        }
    }

    /// <summary>Gets the number of tasks currently scheduled.</summary>
    private int DebugTaskCount => _blockingTaskQueue.Count;

    /// <summary>Queues a task to the scheduler.</summary>
    /// <param name="task">The task to be queued.</param>
    protected override void QueueTask(Task task)
    {
        // If we've been disposed, no one should be queueing
        if (_disposeCancellation.IsCancellationRequested)
        {
            throw new ObjectDisposedException(GetType().Name);
        }

        // add the task to the blocking queue
        _blockingTaskQueue.Add(task);
    }

    /// <summary>Tries to execute a task synchronously on the current thread.</summary>
    /// <param name="task">The task to execute.</param>
    /// <param name="taskWasPreviouslyQueued">Whether the task was previously queued.</param>
    /// <returns>true if the task was executed; otherwise, false.</returns>
    protected override bool TryExecuteTaskInline(Task task, bool taskWasPreviouslyQueued)
    {
        // If we're already running tasks on this threads, enable inlining
        return TaskProcessingThread.Value && TryExecuteTask(task);
    }

    /// <summary>Gets the tasks scheduled to this scheduler.</summary>
    /// <returns>An enumerable of all tasks queued to this scheduler.</returns>
    /// <remarks>This does not include the tasks on sub-schedulers.  Those will be retrieved by the debugger separately.</remarks>
    protected override IEnumerable<Task> GetScheduledTasks()
    {
        // Get the tasks from the blocking queue.
        return _blockingTaskQueue.ToList();
    }

    /// <summary>Gets the maximum concurrency level to use when processing tasks.</summary>
    public override int MaximumConcurrencyLevel { get; }

    /// <summary>Initiates shutdown of the scheduler.</summary>
    public void Dispose()
    {
        _disposeCancellation.Cancel();
    }
}
