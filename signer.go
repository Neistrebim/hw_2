package main

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

var wg = &sync.WaitGroup{}

// ExecutePipeline takes job to implement some data processing
func ExecutePipeline(jobs ...job) {

	initChannels := make(map[string]chan interface{})
	for i := 0; i <= len(jobs); i++ {
		initChannels["pipe"+strconv.Itoa(i)] = make(chan interface{}, MaxInputDataLen)
	}

	// run jobs
	for k, j := range jobs {
		fmt.Println("VALUE for k: ", k)
		if k+1 < len(jobs) {
			in := initChannels["pipe"+strconv.Itoa(k)]
			out := initChannels["pipe"+strconv.Itoa(k+1)]
			fmt.Println(k, "Start gorutine. Chans: ", in, out)
			go j(in, out)
		}
	}

	// SingleHash, MultiHash, CombineResults
	wg.Add(3)
	wg.Wait()
	//time.Sleep(20 * time.Second)

}

// SingleHash does something
func SingleHash(in, out chan interface{}) {
	fmt.Println("Start SingleHash")
	wg1 := &sync.WaitGroup{}
	for i := range in {
		// if len(in) == 0 {
		// 	break
		// }
		wg1.Add(1)
		inputRaw, ok := i.(int)
		if !ok {
			log.Fatal("SingleHash. cant convert input data to string")
		}
		go func() {

			inputString := strconv.Itoa(inputRaw)
			fmt.Println("SingleHash. Got inputRaw: ", inputRaw)
			fmt.Printf("SingleHash. Send to: %#v\n", out)
			fmt.Printf("SingleHash. Receive from: %#v\n", in)
			out <- fmt.Sprintf("%s~%s", DataSignerCrc32(inputString), DataSignerCrc32(DataSignerMd5(inputString)))
			wg1.Done()
		}()

	}
	fmt.Println("Reached wg1.Wait from SingleHash")
	wg1.Wait()

	close(out)
	// from ExecutePipeline
	fmt.Println("Reached wg.Done from SingleHash")
	wg.Done()
}

// MultiHash does something
func MultiHash(in, out chan interface{}) {
	fmt.Println("Start MultiHash")
	fmt.Printf("MultiHash. Read from %#v\n", in)

	mu := &sync.Mutex{}
	wg1 := &sync.WaitGroup{}
	wg2 := &sync.WaitGroup{}
	for i := range in {
		wg2.Add(1)
		inputRaw, ok := i.(string)
		if !ok {
			log.Fatal("MultiHash. cant convert input data to string")
		}
		go func() {
			defer wg2.Done()
			// Reset map for every cycle step (input from a channel).
			DataSignerMap := map[int]string{}
			fmt.Println("MultiHash. Got inputRaw: ", inputRaw)
			fmt.Printf("MultiHash. Send to: %#v\n", out)
			fmt.Printf("MultiHash. Receive from: %#v\n", in)
			for i := 0; i <= 5; i++ {
				wg1.Add(1)
				go func(DataSignerMap map[int]string, i int, mu *sync.Mutex) {
					defer wg1.Done()
					mu.Lock()
					DataSignerMap[i] = DataSignerCrc32(strconv.Itoa(i) + inputRaw)
					mu.Unlock()
				}(DataSignerMap, i, mu)
			}
			wg1.Wait()

			// Get keys from a map
			keys := []int{}
			for k := range DataSignerMap {
				keys = append(keys, k)
			}
			// sort keys
			sort.Ints(keys)
			// form resulted string
			outputString := ""
			for _, k := range keys {
				outputString += DataSignerMap[k]
			}
			out <- outputString
		}()
	}
	wg2.Wait()

	// from ExecutePipeline
	fmt.Println("Reached wg.Done from MultiHash")
	close(out)
	wg.Done()
}

// CombineResults does something
func CombineResults(in, out chan interface{}) {
	fmt.Println("Start ConbineResults")
	finalResutls := []string{}
	for i := range in {
		inputRaw, ok := i.(string)
		fmt.Println("CombineResults. Got inputRaw: ", inputRaw)
		if !ok {
			log.Fatal("CombineResults. cant convert input data to string")
		}
		finalResutls = append(finalResutls, inputRaw)
	}
	sort.Strings(finalResutls)
	fmt.Println("finalResults. Got : ", finalResutls)
	out <- fmt.Sprintf("%s", strings.Join(finalResutls, "_"))

	// from ExecutePipeline
	fmt.Println("Reached wg.Done from Combine")
	close(out)
	wg.Done()
}

func main() {
	testExpected := "1173136728138862632818075107442090076184424490584241521304_1696913515191343735512658979631549563179965036907783101867_27225454331033649287118297354036464389062965355426795162684_29568666068035183841425683795340791879727309630931025356555_3994492081516972096677631278379039212655368881548151736_4958044192186797981418233587017209679042592862002427381542_4958044192186797981418233587017209679042592862002427381542"
	testResult := "NOT_SET"

	//inputData := []int{0, 1, 1, 2, 3, 5, 8}
	inputData := []int{0}

	hashSignJobs := []job{
		job(func(in, out chan interface{}) {
			for _, fibNum := range inputData {
				out <- fibNum
			}
			close(out)
		}),
		job(SingleHash),
		job(MultiHash),
		job(CombineResults),
		job(func(in, out chan interface{}) {
			dataRaw := <-in
			data, ok := dataRaw.(string)
			if !ok {
				fmt.Println("cant convert result data to string")
			}
			testResult = data
		}),
	}

	start := time.Now()

	ExecutePipeline(hashSignJobs...)

	end := time.Since(start)

	expectedTime := 3 * time.Second

	fmt.Println("Result:", testResult)
	if testExpected != testResult {
		fmt.Printf("results not match\nGot: %v\nExpected: %v", testResult, testExpected)
	}

	if end > expectedTime {
		fmt.Printf("execition too long\nGot: %s\nExpected: <%s", end, time.Second*3)
	}

}
