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
	wg *sync.WaitGroup  // wave's wait group
}

// reads jobs from `job` channel, calculates sum and stores it in arr[left];
// after completing the job - calls wg.Done()
func worker(arr []int64, jobs <- chan job, done *sync.WaitGroup) {
	defer done.Done()  // call when out of current scope
	for j := range jobs {
		sum := arr[j.left] + arr[j.right]
		arr[j.left] = sum
		j.wg.Done()
	}
}

// core function for doing parallel sums by waves
func parallelSum(arr []int64, workers int) int64 {
	if len(arr) == 0 {
		return 0
	}
	if len(arr) == 1 {
		return arr[0]
	}

	jobs := make(chan job)  // channel for giving jobs to working threads

	// start pool of workers
	var wgWorkers sync.WaitGroup
	for range workers {
		wgWorkers.Add(1)
		go worker(arr, jobs, &wgWorkers)
	}

	currentLen := len(arr)  // actual lenght, halving each wave

	for currentLen > 1 {
		pairs := currentLen / 2  // number of wave's pairs

		var wave sync.WaitGroup
		wave.Add(pairs)  // for each pair reserve relevant WaitGroup's deltas 

		for i := range pairs {
			left := i
			right := currentLen - 1 - i
			jobs <- job {  // pass a job struct for each pair to the jobs WaitGroup
				left: left,
				right: right,
				wg: &wave,  // reference to current wave
			}
		}
		wave.Wait()  // wait 'till all wave's jobs will complete work

		currentLen = (currentLen + 1) / 2
	}

	close(jobs)  // shutting down the jobs channel

	wgWorkers.Wait()

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

