package throttler

import (
	"fmt"
	"time"
)

const Unrestricted = -1

type CallTimedoutError struct {
	ErrorString string
}

func (err CallTimedoutError) Error() string {
	return err.ErrorString
}

type MaxBacklogReachedError struct {
	ErrorString string
}

func (err MaxBacklogReachedError) Error() string {
	return err.ErrorString
}

type Throttler struct {
	numCallsInLastSecond int

	maxBacklog         int
	maxConcurrent      int
	maxDurationPerCall float64
	maxCallsPerSecond  int
	chNewJob           chan *ThrottleJob
	chCancelJob        chan *ThrottleJob
	chJobEnds          chan *ThrottleJob
	chClose            chan chan int // stop worker

	backlog    []*ThrottleJob
	backlogMap map[*ThrottleJob]bool
	running    map[*ThrottleJob]bool
}

type workFn func() (interface{}, error)

type ThrottleJob struct {
	work        workFn
	Done        chan ThrottleJobRet
	cancel      chan bool         // cancel a running job
	chCancelJob chan *ThrottleJob // saving a copy of the Throttler's cancel channel
}

type ThrottleJobRet struct {
	Ret interface{}
	Err error
}

func CreateThrottler(maxBacklog int, maxConcurrent int, maxDurationPerCall float64, maxCallsPerSecond int) *Throttler {
	// sanity check for values
	if maxBacklog < Unrestricted {
		maxBacklog = Unrestricted
	}

	if maxConcurrent <= 0 {
		maxConcurrent = Unrestricted
	}

	if maxDurationPerCall <= 0 {
		maxDurationPerCall = Unrestricted
	}

	if maxCallsPerSecond <= 0 {
		maxCallsPerSecond = Unrestricted
	}

	t := &Throttler{0, maxBacklog, maxConcurrent, maxDurationPerCall, maxCallsPerSecond, make(chan *ThrottleJob, 1), make(chan *ThrottleJob, 1), make(chan *ThrottleJob, 1), make(chan chan int, 1), make([]*ThrottleJob, 0, 10), map[*ThrottleJob]bool{}, map[*ThrottleJob]bool{}}
	go t.worker()
	return t
}

func (t *Throttler) ScheduleToRun(work func() (interface{}, error)) *ThrottleJob {
	tj := &ThrottleJob{work, make(chan ThrottleJobRet, 1), make(chan bool, 1), t.chCancelJob}
	t.chNewJob <- tj
	return tj
}

// returns # of jobs left
func (t *Throttler) Close() int {
	waitClose := make(chan int)
	t.chClose <- waitClose
	return <-waitClose
}

func (tj *ThrottleJob) WaitForCompletion() (interface{}, error) {
	ret := <-tj.Done
	return ret.Ret, ret.Err
}

func (tj *ThrottleJob) Cancel() {
	tj.chCancelJob <- tj
}

// very local debugging
func throttlerDebug(format string, a ...interface{}) {
	////fmt.Printf(format, a...)
}

// only call this if we allow backlogging
// check for queue limit here, return error here if no more room
func (t *Throttler) workerQueueJob(newJob *ThrottleJob) {
	throttlerDebug("maxBacklog=%d backlogSize=%d\n", t.maxBacklog, len(t.backlogMap))

	if t.maxBacklog > 0 && (len(t.backlogMap) >= t.maxBacklog) {
		throttlerDebug("W - unable to queued, max backlog reached\n")
		// no room on queue to handle
		newJob.Done <- ThrottleJobRet{nil, MaxBacklogReachedError{fmt.Sprintf("Max backlog reached = %d", t.maxBacklog)}}
		return
	}

	throttlerDebug("W - job queued\n")
	t.backlog = append(t.backlog, newJob)
	t.backlogMap[newJob] = true
}

// returns nil if we don't have a next job
func (t *Throttler) workerPopNextJob() *ThrottleJob {

	for len(t.backlog) > 0 {
		job := t.backlog[0]
		if _, ok := t.backlogMap[job]; ok {
			// found it
			t.backlog = t.backlog[1:]
			delete(t.backlogMap, job)
			return job
		}

		// not in map, this was probably a canceled job, keep going
		t.backlog = t.backlog[1:]
	}

	return nil
}

func (t *Throttler) workerRunJob(newJob *ThrottleJob) {
	if newJob == nil {
		return
	}

	t.running[newJob] = true
	t.numCallsInLastSecond++

	go func() {
		chResults := make(chan ThrottleJobRet, 1)
		go func() {
			ret, err := newJob.work()
			chResults <- ThrottleJobRet{ret, err}
		}()

		callTimeoutChan := make(<-chan time.Time)
		if t.maxDurationPerCall > 0 {
			callTimeoutChan = time.After(time.Duration(t.maxDurationPerCall * float64(time.Second)))
		}

		select {
		case <-callTimeoutChan:
			throttlerDebug("call timed out\n")
			newJob.Done <- ThrottleJobRet{nil, CallTimedoutError{fmt.Sprintf("Call timed out. (Max duration per call in seconds = %v)", t.maxDurationPerCall)}}
		case <-newJob.cancel:
			newJob.Done <- ThrottleJobRet{nil, fmt.Errorf("Job canceled by original requester.")}
		case retJob := <-chResults:
			newJob.Done <- retJob
		}
		t.chJobEnds <- newJob // let worker know we are done
	}()
}

func (t *Throttler) workerCanMakeCall() bool {
	throttlerDebug("maxConcurrent=%d num_running=%d   maxCallsPerSecond=%d numCallsInLastSecond=%d\n", t.maxConcurrent, len(t.running), t.maxCallsPerSecond, t.numCallsInLastSecond)

	// checking concurrency AND exceeding limit
	if t.maxConcurrent != Unrestricted && (len(t.running) >= t.maxConcurrent) {
		return false
	}

	// rate limiting?
	if t.maxCallsPerSecond != Unrestricted && (t.numCallsInLastSecond >= t.maxCallsPerSecond) {
		return false
	}

	return true
}

func (t *Throttler) workerScheduleMoreJobs() {
	for t.workerCanMakeCall() {
		nextQueueJob := t.workerPopNextJob()
		if nextQueueJob == nil {
			return
		}
		throttlerDebug("W - run next job\n")
		t.workerRunJob(nextQueueJob)
	}

}

func (t *Throttler) worker() {
	rateResetter := time.Tick(time.Second)
	t.numCallsInLastSecond = 0

	for {
		select {
		case <-rateResetter:
			throttlerDebug("W - rate reset\n")
			t.numCallsInLastSecond = 0
			t.workerScheduleMoreJobs()

		case newJob := <-t.chNewJob:
			throttlerDebug("W - new job\n")

			if t.maxBacklog == 0 {
				// no backlog allowed
				if !t.workerCanMakeCall() {
					// fail immediately
					newJob.Done <- ThrottleJobRet{nil, MaxBacklogReachedError{fmt.Sprintf("Max backlog is 0, but we are unable to make call now.")}}
					break
				}

				// run immediately
				t.workerRunJob(newJob)
			} else {
				t.workerQueueJob(newJob)
				t.workerScheduleMoreJobs()
			}

		case endedJob := <-t.chJobEnds:
			throttlerDebug("W - job ended\n")
			delete(t.running, endedJob)
			t.workerScheduleMoreJobs()

		case canceledJob := <-t.chCancelJob:
			throttlerDebug("W - job canceled \n")
			delete(t.backlogMap, canceledJob)
			delete(t.running, canceledJob)
			canceledJob.cancel <- true
			t.workerScheduleMoreJobs()

		case closeResp := <-t.chClose:
			// done!
			closeResp <- (len(t.running) + len(t.backlogMap))
			return
		}
	}
}
