using System.Collections.Concurrent;
using System.Diagnostics;

class Program
{
    static void Main(string[] args)
    {
        int N = 2_000_000;
        int workerCount = Environment.ProcessorCount;

        Console.WriteLine($"N = {N}, workerCount = {workerCount}");

        var rnd = new Random();
        double[] original = new double[N];
        for (int i = 0; i < N; i++)
            original[i] = rnd.Next(0, 1000) / 1000.0;

        // copies for parallel and sequential runs
        double[] seqArray = new double[N];
        double[] parArray = new double[N];
        Array.Copy(original, seqArray, N);
        Array.Copy(original, parArray, N);

        // sequential run
        var stopwatch = Stopwatch.StartNew();
        double seqResult = SequentialReduce(seqArray);
        stopwatch.Stop();
        var seqTime = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Sequential result = {seqResult:F6}, time = {seqTime:F2} ms");

        // parallel run + dispatcher
        stopwatch.Restart();
        double parResult = ParallelReduce(parArray, workerCount);
        stopwatch.Stop();
        var parTime = stopwatch.ElapsedMilliseconds;
        Console.WriteLine($"Parallel result   = {parResult:F6}, time = {parTime:F2} ms");

        // sums check
        double eps = 1e-9;
        if (Math.Abs(seqResult - parResult) <= eps)
            Console.WriteLine("Sums are the same");
        else
            Console.WriteLine($"Sums are NOT the same (diff = {Math.Abs(seqResult - parResult)})");
    }

    static double SequentialReduce(double[] array)
    {
        int length = array.Length;
        while (length > 1)
        {
            int pairs = length / 2;
            for (int leftIndex = 0; leftIndex < pairs; leftIndex++)
            {
                int rightIndex = length - 1 - leftIndex;
                array[leftIndex] += array[rightIndex];
            }
            // new len - firts half + mid element (if len is not prime)
            length = (length + 1) / 2;
        }
        return array.Length > 0 ? array[0] : 0.0;
    }

    static double ParallelReduce(double[] array, int workersCount)
    {
        int length = array.Length;
        if (length == 0) return 0.0;
        if (length == 1) return array[0];

        var cts = new CancellationTokenSource();  // for signalling workers to stop, when work is done
        var queue = new ConcurrentQueue<(int leftIndex, int waveLength)>();  // tasks for workers, where each one is tuple of leftIndex and current waveLength
        var semaphore = new SemaphoreSlim(0);  // count avaliable queued tasks, workers wait on it and are released when Release() is called
        CountdownEvent countdown = null;  // one instance per wave, used by dispatcher to wait untill all pairs in wave have been processed

        Task[] workers = new Task[workersCount];
        for (int workerIndex = 0; workerIndex < workersCount; workerIndex++)
        {
            workers[workerIndex] = Task.Run(() =>
            {
                try
                {
                    while (true)
                    {
                        // smph
                        semaphore.Wait(cts.Token);  // blocks, untill task is awaliable or cancellation requested

                        // cts
                        if (cts.IsCancellationRequested)
                            break;

                        if (queue.TryDequeue(out var task))  // take a queued task if any
                        {
                            int leftIndex = task.leftIndex;
                            int waveLength = task.waveLength;
                            int rightIndex = waveLength - 1 - leftIndex;

                            array[leftIndex] += array[rightIndex];

                            // ctd
                            countdown?.Signal();  // inform the dispatcher, that this pair is done
                        }
                    }
                }
                catch (OperationCanceledException) { }  // exit cleanly if the wait was cancelled
            // cts
            }, cts.Token);
        }

        int waveLength = length;
        while (waveLength > 1)
        {
            int pairs = waveLength / 2;
            // ctd
            countdown = new CountdownEvent(pairs);  // new countdown from pairs of current wave

            for (int leftIndex = 0; leftIndex < pairs; leftIndex++)
            {
                queue.Enqueue((leftIndex, waveLength));
                // smph
                semaphore.Release();  // release a permit so one worker can wake and pick the task
            }

            // ctd
            countdown.Wait();  // block the dispatcher untill all pairs calls to countdown.Signal() have occured - all tasks in the wave completed their work

            waveLength = (waveLength + 1) / 2;
        }

        // cts
        cts.Cancel();  // request workers termination

        // run Release() once for each worker so that blocked ones (line 84) can proceed and observe the cancellation (line 87)
        for (int workerIndex = 0; workerIndex < workersCount; workerIndex++)
            semaphore.Release();

        try
        {
            Task.WaitAll(workers);
        }
        catch (AggregateException) { }

        return array[0];
    }
}

