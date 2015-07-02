package throttler

import (
	_ "fmt"
	"testing"
	"time"
)

const (
	evJobSchedule = "started"
	evJobCancel   = "canceled"
	evJobEnded    = "ended"
)

// the sequece of things which we initiate/wait to happen
type testEvent struct {
	evTime            float64 // from test start, seconds
	evType            string
	jobName           string
	jobLen            float64 // evJobSchedule: number of seconds it is supposed to run
	jobEndedWithError bool    // evJobEnded: are we expecting an error or not
}

type scheduledRet struct {
	jobName string
	job     *ThrottleJob
}

// once it's running, we really can't stop, but it's not terrible, but may have unexpected results
func makeWork(name string, durationSecond float64) workFn {
	return func() (interface{}, error) {
		//Z fmt.Printf("start_test_work: "+name+" duration %f\n", durationSecond)
		time.Sleep(time.Duration(durationSecond * float64(time.Second)))
		//Z fmt.Printf("end_test_work: " + name + "\n")
		return name, nil
	}
}

func runTest(t *testing.T, throttler *Throttler, evs []testEvent) []testEvent {
	observed := make([]testEvent, 0, len(evs)*2)
	starts := make([]testEvent, 0, len(evs))
	cancels := map[string]testEvent{} // by job

	scheduled := map[string]*ThrottleJob{}
	resultChan := make(chan testEvent, len(evs))
	scheduledChan := make(chan scheduledRet, len(evs))

	// pull out all the starts and cancels, then kick them off all in a row
	for _, te := range evs {
		switch te.evType {
		case evJobSchedule:
			starts = append(starts, te)
		case evJobCancel:
			cancels[te.jobName] = te
		}
	}

	// in practice, we can only properly cancel if it is in the throttler
	// queue, not if it is running (i.e. not 100% transactional)

	testEpoch := time.Now()

	// don't worry about drift for now
	for _, startTe := range starts {

		go func(te testEvent) {
			<-time.After(time.Duration(te.evTime * float64(time.Second)))

			// at the scheduled time
			resultChan <- testEvent{time.Now().Sub(testEpoch).Seconds(), evJobSchedule, te.jobName, te.jobLen, false}
			j := throttler.ScheduleToRun(makeWork(te.jobName, te.jobLen))
			scheduledChan <- scheduledRet{te.jobName, j}

			// do we have to worry about cancels?
			cancelTe, ok := cancels[te.jobName]
			if ok {
				go func() {
					elapsed := time.Now().Sub(testEpoch)
					relTimeLeft := time.Duration(cancelTe.evTime*float64(time.Second)) - elapsed
					<-time.After(relTimeLeft)
					resultChan <- testEvent{time.Now().Sub(testEpoch).Seconds(), evJobCancel, cancelTe.jobName, 0, false}
					j.Cancel()
				}()
			}

			go func() {
				// wait for result
				_, err := j.WaitForCompletion()
				endTime := time.Now().Sub(testEpoch).Seconds()
				resultChan <- testEvent{endTime, evJobEnded, te.jobName, 0, err != nil}
			}()
		}(startTe)
	}

	testEnd := time.After(time.Duration((evs[len(evs)-1].evTime + 1.0) * float64(time.Second)))

OuterLoop:
	for {
		select {
		case j := <-scheduledChan:
			scheduled[j.jobName] = j.job
		case observedEv := <-resultChan:
			observed = append(observed, observedEv)
		case <-testEnd:
			break OuterLoop
		}
	}

	numStillRunning := throttler.Close()
	if numStillRunning > 0 {
		t.Fatalf("End of test, expecting throttler to be idle, but there are still %d jobs.\n", numStillRunning)
	}

	return observed
}

func compareEvs(t *testing.T, expected []testEvent, observed []testEvent) {
	// round observed times to nearest 0.1s
	for i, _ := range observed {
		observed[i].evTime = float64(int((observed[i].evTime+0.05)*10)) / 10.0
	}

	t.Logf("expected:\n%v\n", expected)
	t.Logf("observed:\n%v\n", observed)

	if len(expected) != len(observed) {
		t.Fatal("expected and observed events don't match")
	}

	// some events happen on the "same" point in the timeline
	// this compare needs to resolve this during the comparisons
	// the simple thing, is just to put it all in map
	expectedMap := map[testEvent]bool{}
	for _, te := range expected {
		expectedMap[te] = true
	}

	isSame := true

	for _, oe := range observed {
		if _, ok := expectedMap[oe]; !ok {
			isSame = false
			break
		}
		delete(expectedMap, oe)
	}

	if len(expectedMap) > 0 {
		isSame = false // left over expected events
	}

	// if they don't match print out expected and observed so we can compare
	// side by side
	if !isSame {
		t.Fatal("expected and observed events don't match")
	}
}

func TestSimpleOneJob(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 3.0, false},
		{3.0, evJobEnded, "a", 0.0, false},
	}
	throttler := CreateThrottler(Unrestricted, Unrestricted, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestSimpleMulti(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 1.0, false},
		{0.5, evJobSchedule, "b", 1.0, false},
		{1.0, evJobSchedule, "c", 1.0, false},
		{1.0, evJobEnded, "a", 0.0, false},
		{1.5, evJobEnded, "b", 0.0, false},
		{2.0, evJobEnded, "c", 0.0, false},
	}
	throttler := CreateThrottler(Unrestricted, Unrestricted, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestSimpleMulti2(t *testing.T) {
	evs := []testEvent{
		{1.0, evJobSchedule, "a", 5.0, false},
		{1.1, evJobSchedule, "b", 0.5, false},
		{1.2, evJobSchedule, "c", 0.5, false},
		{1.6, evJobEnded, "b", 0.0, false},
		{1.7, evJobEnded, "c", 0.0, false},
		{6.0, evJobEnded, "a", 0.0, false},
	}
	throttler := CreateThrottler(Unrestricted, Unrestricted, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestConcurrent(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 2.0, false},
		{0.1, evJobSchedule, "b", 2.0, false},
		{0.2, evJobSchedule, "c", 2.0, false},
		{2.0, evJobEnded, "a", 0.0, false},
		{4.0, evJobEnded, "b", 0.0, false},
		{6.0, evJobEnded, "c", 0.0, false},
	}
	throttler := CreateThrottler(Unrestricted, 1, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestRateLimiter(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 2.0, false},
		{0.1, evJobSchedule, "b", 2.0, false},
		{0.2, evJobSchedule, "c", 2.0, false},
		{2.0, evJobEnded, "a", 0.0, false},
		{3.0, evJobEnded, "b", 0.0, false},
		{4.0, evJobEnded, "c", 0.0, false},
	}
	throttler := CreateThrottler(Unrestricted, Unrestricted, Unrestricted, 1)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestMaxCallDuration(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 10.0, false},
		{2.0, evJobEnded, "a", 0.0, true}, // le timeout
	}
	throttler := CreateThrottler(Unrestricted, Unrestricted, 2, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestMaxBacklog(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 3.0, false},
		{0.1, evJobSchedule, "b", 3.0, false},
		{0.2, evJobSchedule, "c", 3.0, false},
		{0.2, evJobEnded, "c", 0.0, true}, // maxbacklog exceeded
		{3.0, evJobEnded, "a", 0.0, false},
		{6.0, evJobEnded, "b", 0.0, false}, // because of 1 concurrent, has to wait for "a" to end first
	}

	// maxbacklog = 1; concurrent = 1 to force backlogging
	throttler := CreateThrottler(1, 1, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestMaxBacklogMany(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 1.0, false},
		{0.0, evJobSchedule, "b", 1.0, false},
		{0.0, evJobSchedule, "c", 1.0, false},
		{0.0, evJobSchedule, "d", 1.0, false},
		{0.0, evJobSchedule, "e", 1.0, false},
		{0.0, evJobSchedule, "f", 1.0, false},
		{0.0, evJobSchedule, "g", 1.0, false},
		{0.0, evJobSchedule, "h", 1.0, false},

		{1.0, evJobEnded, "a", 0.0, false}, // ended doesn't use duration
		{1.0, evJobEnded, "b", 0.0, false},
		{1.0, evJobEnded, "c", 0.0, false},
		{1.0, evJobEnded, "d", 0.0, false},
		{1.0, evJobEnded, "e", 0.0, false},
		{1.0, evJobEnded, "f", 0.0, false},
		{1.0, evJobEnded, "g", 0.0, false},
		{1.0, evJobEnded, "h", 0.0, false},
	}

	throttler := CreateThrottler(Unrestricted, Unrestricted, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestMaxBacklogZero(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 3.0, false},
		{0.1, evJobSchedule, "b", 3.0, false},
		{0.1, evJobEnded, "b", 0.0, true}, // nil backlog, fail imeediately
		{3.0, evJobEnded, "a", 0.0, false},
	}

	// maxbacklog = 1; concurrent = 1 to force backlogging
	throttler := CreateThrottler(0, 1, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}

func TestCancels(t *testing.T) {
	evs := []testEvent{
		{0.0, evJobSchedule, "a", 7.0, false},
		{0.1, evJobSchedule, "b", 7.0, false},
		{2.0, evJobCancel, "b", 0.0, false},
		{2.0, evJobEnded, "b", 0.0, true},
		{7.0, evJobEnded, "a", 0.0, false},
	}

	throttler := CreateThrottler(1, Unrestricted, Unrestricted, Unrestricted)
	observed := runTest(t, throttler, evs)
	compareEvs(t, evs, observed)
}
