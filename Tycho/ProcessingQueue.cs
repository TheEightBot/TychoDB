using System;
using System.Threading.Tasks.Dataflow;
using System.Threading.Tasks;
using System.Threading;
using System.Runtime.Serialization;
using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("EightBot.Orbit.Tests")]
namespace EightBot.Orbit.Client
{
    internal class ProcessingQueue
    {
        private readonly ActionBlock<QueuedTask> _taskProcessing;

        public ProcessingQueue()
        {
            _taskProcessing =
                new ActionBlock<QueuedTask>(
                    ProcessQueuedTask,
                    new ExecutionDataflowBlockOptions
                    {
                        EnsureOrdered = true,
                        MaxDegreeOfParallelism = 1
                    });
        }

        public async Task Queue(Action processingTask, CancellationToken cancellationToken = default(CancellationToken))
        {
            var queuedTask =
                new QueuedTask
                {
                    TaskRunner = 
                        _ =>
                        {
                            processingTask.Invoke();
                            return Task.FromResult(_);
                        }
                };

            if (cancellationToken == default(CancellationToken))
                cancellationToken = CancellationToken.None;

            var queued = await _taskProcessing.SendAsync(queuedTask, cancellationToken).ConfigureAwait(false);

            if (!queued)
            {
                throw new QueueFailureException("Unable to queue task");
            }

            cancellationToken.CheckIfCancelled();

            await queuedTask.CompletionSource.Task.ConfigureAwait(false);

            cancellationToken.CheckIfCancelled();
        }

        public async Task<T> Queue<T>(Func<T> processingTask, CancellationToken cancellationToken = default(CancellationToken))
        {
            var queuedTask =
                new QueuedTask
                {
                    TaskRunner =
                        _ =>
                        {
                            var processingResult = processingTask.Invoke();
                            return Task.FromResult((object)processingResult);
                        }
                };

            if (cancellationToken == default(CancellationToken))
                cancellationToken = CancellationToken.None;

            var queued = await _taskProcessing.SendAsync(queuedTask, cancellationToken).ConfigureAwait(false);

            if (!queued)
            {
                throw new QueueFailureException("Unable to queue task");
            }

            cancellationToken.CheckIfCancelled();

            var result = await queuedTask.CompletionSource.Task.ConfigureAwait(false);

            cancellationToken.CheckIfCancelled();

            return (T)result;
        }

        public async Task<T> Queue<T>(Func<Task<T>> processingTask, CancellationToken cancellationToken = default(CancellationToken))
        {
            var queuedTask =
                new QueuedTask
                {
                    TaskRunner =
                        async _ =>
                        {
                            return await processingTask.Invoke().ConfigureAwait(false);
                        }
                };

            if (cancellationToken == default(CancellationToken))
                cancellationToken = CancellationToken.None;

            var queued = await _taskProcessing.SendAsync(queuedTask, cancellationToken).ConfigureAwait(false);

            if (!queued)
            {
                throw new QueueFailureException("Unable to queue task");
            }

            cancellationToken.CheckIfCancelled();

            var result = await queuedTask.CompletionSource.Task.ConfigureAwait(false);

            cancellationToken.CheckIfCancelled();

            return (T)result;
        }

        public async Task Queue<T>(T input, Func<T, Task> processingTask, CancellationToken cancellationToken = default(CancellationToken))
        {
            var queuedTask =
                new QueuedTask
                {
                    Input = input,
                    TaskRunner = 
                        async x =>
                        {
                            await processingTask.Invoke((T)x).ConfigureAwait(false);
                            return default(object);
                        }
                };

            if (cancellationToken == default(CancellationToken))
                cancellationToken = CancellationToken.None;

            var queued = await _taskProcessing.SendAsync(queuedTask, cancellationToken).ConfigureAwait(false);

            if (!queued)
            {
                throw new QueueFailureException("Unable to queue task");
            }

            cancellationToken.CheckIfCancelled();

            await queuedTask.CompletionSource.Task.ConfigureAwait(false);

            cancellationToken.CheckIfCancelled();
        }

        public async Task<TResult> Queue<TInput, TResult>(TInput input, Func<TInput, Task<TResult>> processingTask, CancellationToken cancellationToken = default(CancellationToken))
        {
            var queuedTask =
                new QueuedTask
                {
                    Input = input,
                    TaskRunner = 
                        async x =>
                        {
                            var result = await processingTask.Invoke((TInput)x).ConfigureAwait(false);
                            return Task.FromResult<object>(result);
                        }
                };

            if (cancellationToken == default(CancellationToken))
                cancellationToken = CancellationToken.None;

            var queued = await _taskProcessing.SendAsync(queuedTask, cancellationToken).ConfigureAwait(false);

            if (!queued)
            {
                throw new QueueFailureException("Unable to queue task");
            }

            cancellationToken.CheckIfCancelled();

            var processingResult = await queuedTask.CompletionSource.Task.ConfigureAwait(false);

            cancellationToken.CheckIfCancelled();

            return (TResult)processingResult;
        }

        private async Task ProcessQueuedTask(QueuedTask input)
        {
            try
            {
                var result = await input.TaskRunner.Invoke(input.Input).ConfigureAwait(false);

                input.CompletionSource.TrySetResult(result);
            }
            catch (Exception ex)
            {
                input.CompletionSource.TrySetException(ex);
            }
        }
    }

    public static class QueueExtensions
    {
        public static void CheckIfCancelled(this CancellationToken cancellationToken)
        {
            if (cancellationToken != CancellationToken.None && !cancellationToken.IsCancellationRequested)
            {
                throw new QueueFailureException("The queued task was cancelled");
            }
        }
    }

    public class QueuedTask
    {
        public TaskCompletionSource<object> CompletionSource { get; set; }
            = new TaskCompletionSource<object>();

        public object Input { get; set; }
        public object Output { get; set; }

        public Func<object, Task<object>> TaskRunner { get; set; }
    }

    public class QueueFailureException : Exception
    {
        public QueueFailureException()
        {
        }

        public QueueFailureException(string message) : base(message)
        {
        }

        public QueueFailureException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected QueueFailureException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}
