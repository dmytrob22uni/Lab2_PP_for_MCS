package main;

import (
	"flag"
	"fmt"
	"runtime"
	"sync"
	"time"
)

type job struct {
	left int
	right int
	wave *sync.WaitGroup  // wave's wait group
}

// reads jobs from `job` channel, calculates sum and stores it in arr[left];
// after completing the job - calls wg.Done()
func worker(arr []int64, jobsChn <- chan job, work *sync.WaitGroup) {
	defer work.Done()  // call when out of current scope
	for job := range jobsChn {  // locked and waiting for data from jobsChn channel; each time new job is added - new cycle of calculations starts
		sum := arr[job.left] + arr[job.right]
		arr[job.left] = sum
		job.wave.Done()
	}
}

// core function for doing parallel sums by waves
func parallelSum(arr []int64, nWorkers int) int64 {
	if len(arr) == 0 {
		return 0
	}
	if len(arr) == 1 {
		return arr[0]
	}

	jobsChn := make(chan job)  // channel for giving jobs to working threads

	// start work WaitGroup, where jobs are being done as received
	var work sync.WaitGroup
	for range nWorkers {
		work.Add(1)
		go worker(arr, jobsChn, &work)
	}

	currentLen := len(arr)  // actual lenght, halving each wave

	for currentLen > 1 {
		pairs := currentLen / 2  // number of wave's pairs

		var wave sync.WaitGroup
		wave.Add(pairs)  // for each pair reserve relevant WaitGroup's deltas 

		for i := range pairs {
			left := i
			right := currentLen - 1 - i
			jobsChn <- job {  // pass new job struct for each pair to the jobsChn channel, which is making its deed in work WaitGroup
				left: left,
				right: right,
				wave: &wave,  // reference to current wave
			}
		}

		wave.Wait()  // wait 'till all wave's jobs will complete work

		currentLen = (currentLen + 1) / 2
	}

	close(jobsChn)  // shutting down the jobs channel, 'cause we are done here and no more jobs are expected

	work.Wait()  // wait till main work WaitGroup finishes jobs calculations if any

	return arr[0]
}

func main() {
	nWorkers := flag.Int("workers", runtime.NumCPU(), "number of workers goroutine to use")
	flag.Parse()

	nItems := 2_000_000
	arr := make([]int64, nItems)
	for i := range nItems {
		arr[i] = int64(i % 1000)
	}

	t0 := time.Now()
	totalSum :=  parallelSum(arr, *nWorkers)
	fmt.Printf("Time elapsed is %dms\n", time.Since(t0).Milliseconds())
	fmt.Printf("Total sum is %d\n\n", totalSum)

	var totalSumCheck int64
	t1 := time.Now()
	for i := range nItems {
		totalSumCheck += int64(i % 1000)
	}
	fmt.Printf("Time elapsed is %dms\n", time.Duration(time.Since(t1).Milliseconds()))
	fmt.Printf("Total sum check is %d\n", totalSumCheck)

	if totalSum != totalSumCheck {
		panic("Sums are not equal!")
	}

}

