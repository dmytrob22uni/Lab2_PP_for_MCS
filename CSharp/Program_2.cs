using System.Diagnostics;

int procCount = Environment.ProcessorCount;
Console.WriteLine($"processors: {procCount}");

int N = 10_000_000;

Console.WriteLine($"N: {N}");

long[] original = new long[N];
for (int i = 0; i < N; i++) original[i] = i % 1000;

long[] CloneOriginal()
{
    var a = new long[original.Length];
    Array.Copy(original, a, original.Length);
    return a;
}

static long LinearSum(long[] a)
{
    long s = 0;
    for (int i = 0; i < a.Length; i++)
        s += a[i];
    return s;
}

static long WaveReduceInPlace(long[] arr, bool parallel, int maxDegreeOfParallelism = -1)
{
    int length = arr.Length;
    while (length > 1)
    {
        int pairs = length / 2;
        if (parallel)
        {
            var po = new ParallelOptions();
            if (maxDegreeOfParallelism > 0)
                po.MaxDegreeOfParallelism = maxDegreeOfParallelism;
            Parallel.For(0, pairs, po, i =>
            {
                arr[i] = arr[i] + arr[length - 1 - i];
            });
        }
        else
        {
            for (int i = 0; i < pairs; i++)
                arr[i] = arr[i] + arr[length - 1 - i];
        }

        length = pairs + (length & 1);  // (pairs & 1) in case length is odd, so we wont lose the remainder
    }

    return arr[0];
}

// run action reps times, return best measured time and result
static (TimeSpan best, long result) Benchmark(
        Func<long[]> inputFactory,
        Func<long[], long> action,
        int reps = 3)
{
    TimeSpan bestTime = TimeSpan.MaxValue;
    long lastResult = 0;
    for (int r = 0; r < reps; r++)
    {
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        var arr = inputFactory();
        var sw = Stopwatch.StartNew();
        long res = action(arr);
        sw.Stop();

        if (sw.Elapsed < bestTime)
            bestTime = sw.Elapsed;
        lastResult = res;
    }

    return (bestTime, lastResult);
}

Console.WriteLine();
Console.WriteLine("benchmarks");

// linear sum
var linear = Benchmark(
        CloneOriginal,
        // lambda wrapper for Func<long[], long> action
        a => LinearSum(a),
        reps: 3);
Console.WriteLine($"{"linear sum:", -41} {linear.result, -12} | best time = {linear.best.TotalMilliseconds:F3} ms");

// sequential wave (single-threaded)
var seqWave = Benchmark(
        CloneOriginal,
        // lambda wrapper for Func<long[], long> action
        a => WaveReduceInPlace(a, parallel: false),
        reps: 3);
Console.WriteLine($"{"wave (sequential):", -41} {seqWave.result, -12} | best time = {seqWave.best.TotalMilliseconds:F3} ms");

// parallel wave (multi-threaded)
var parWave = Benchmark(
        CloneOriginal,
        // lambda wrapper for Func<long[], long> action
        a => WaveReduceInPlace(a, parallel: true, maxDegreeOfParallelism: procCount),
        reps: 3);
Console.WriteLine($"{"wave (parallel, parallelism degree = ", -37}{procCount, -2}): {parWave.result, -12} | best time = {parWave.best.TotalMilliseconds:F3} ms");

if (linear.result != seqWave.result || linear.result != parWave.result)
{
    Console.WriteLine();
    Console.WriteLine("Validation: WRONG");
    Console.WriteLine($"linear: {linear.result}\nsequential wave: {seqWave.result}\nparallel wave: {parWave.result}");
}
else
{
    Console.WriteLine();
    Console.WriteLine("Validation: OK");
}

