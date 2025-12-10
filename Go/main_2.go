package main;

import (
	"fmt"
	"runtime"
	"sync"
	"time"
)

func fillArray(N int) []int64 {
	arr := make([]int64, N)
	for i := 0; i < N; i++ {
		arr[i] = int64(i % 1000)
	}
	return arr
}

func waveSumSeq(arr []int64) int64 {
	n := len(arr)
	for n > 1 {
		left := n / 2
		for i := 0; i < left; i++ {
			arr[i] = arr[i] + arr[n-1-i]
		}
		n = left + (n & 1)  // trick to keep remainder if n is odd in this iteration
	}
	return arr[0]
}

func waveSumParallel(arr []int64, workers int) int64 {
	n := len(arr)
	for n > 1 {
		left := n / 2
		chunkSize := (left + workers - 1) / workers

		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			start := w * chunkSize
			end := start + chunkSize
			if end > left {
				end = left
			}
			if start >= end {
				break
			}
			wg.Add(1)
			// capture local copy of n for closure, because n changes each iteration
			go func(s, e, curN int) {
				defer wg.Done()
				for i := s; i < e; i++ {
					arr[i] = arr[i] + arr[curN-1-i]
				}
			}(start, end, n)  // parameters to pass into the func
		}
		wg.Wait()
		n = left + (n & 1)  // trick to keep remainder if n is odd in this iteration
	}
	return arr[0]
}

func expectedSum(N int) int64 {
	// 0, 1, 2, …, 998, 999, 0, 1, 2, …, 999, 0, 1, 2, …, rem-1
	cycleSum := int64(999 * 1000 / 2)  // sum of sequence 1..999: k(k+1)/2
	cycles := N / 1000
	rems := N % 1000
	remSum := int64((rems - 1) * rems / 2)  // sum of sequence 0..rem-1: k(k+1)/2
	// 11: / 3
	// 0, 1, 2, 0, 1, 2, 0, 1, 2 | 0, 1
	// len=2 rems number
	// but we are counting from 0, so last number is rems-1
	return int64(cycles)*cycleSum + remSum
}

func main() {
	N := 10_000_000

	cpus := runtime.NumCPU()
	runtime.GOMAXPROCS(cpus)
	fmt.Printf("processes: %d\n", cpus)
	fmt.Printf("N: %d\n", N)
	fmt.Printf("\n")

	orig := fillArray(N)

	arrSeq := make([]int64, N)
	copy(arrSeq, orig)

	arrPar := make([]int64, N)
	copy(arrPar, orig)

	// sequential
	start := time.Now()
	sumSeq := waveSumSeq(arrSeq)
	durSeq := time.Since(start)

	// parallel
	start = time.Now()
	sumPar := waveSumParallel(arrPar, cpus)
	durPar := time.Since(start)

	// expected sum
	exp := expectedSum(N)

	fmt.Printf("results:\n")
	fmt.Printf("sequential sum: %v, time = %v\n", sumSeq, durSeq)
	fmt.Printf("parallel sum:   %v, time = %v\n", sumPar, durPar)
	fmt.Printf("expected sum:   %v\n", exp)

	// check
	if sumSeq != exp || sumPar != exp {
		fmt.Println("\ncheck: MISMATCH")
		if sumSeq != exp {
			fmt.Printf(" seq != exp (seq=%d, exp=%d)\n", sumSeq, exp)
		}
		if sumPar != exp {
			fmt.Printf(" par != exp (par=%d, exp=%d)\n", sumPar, exp)
		}
	} else {
		speedup := float64(durSeq) / float64(durPar)
		fmt.Printf("\ncheck: OK")
		fmt.Printf("\nspeedup: %.3fx\n", speedup)
	}
}

