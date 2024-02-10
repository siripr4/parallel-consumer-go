package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// - [x] Poll Kafka
// - [x] Create if not exists and push to topic-partition channel
// - [ ] Commit offsets for al least once guarantee
// - [ ] Listen to rebalance events
// - [ ] Create new topic partition worker queue
//  	- Tear down work queue in case of partition revoke
// - [ ] Handle backpressure
// 		- Pause polling
// 		- Resume polling

// Prior to a rebalance event,
// Poller set a hard deadline and notifies Executor to wrap up its in-flight processing
// and Offset Manager to follow up with a last commit.
// If the deadline has passed, or Poller has received responses from others,
// it takes down the work queues and gets back to wait for rebalance.
var (
	seedBrokers = "localhost:9092"
	topic       = "topic to consume from"
	group       = "group to consume within"
)

type worker func(ctx context.Context, record *kgo.Record) error

// Test func
func process(ctx context.Context, record *kgo.Record) error {
	select {
	case <-ctx.Done():
		// Handle cancellation, cleanup, or return accordingly
		return ctx.Err()
	default:
		// Continue with the normal logic of your function
		fmt.Printf("\nProcessing record with key: %s", string(record.Value))
		return nil
	}
}

func main() {
	seeds := []string{"localhost:9092"}
	client, err := kgo.NewClient(
		kgo.DisableAutoCommit(),
		kgo.SeedBrokers(seeds...),
		kgo.ConsumerGroup("parallel-consumer-0"),
		kgo.ConsumeTopics("parallel-consumer"),
	)
	fmt.Println("Connected to Kafka")
	if err != nil {
		panic(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chanMap := setUpChannels(2)
	defer closeChannels(chanMap)

	processedRecordsChan := make(chan *kgo.Record, 10)
	defer close(processedRecordsChan)

	// Set up executor, this is currently 1:1 with work queues
	for _, v := range chanMap {
		execute(v, process, processedRecordsChan)
	}

	go commitRecords(ctx, client, processedRecordsChan)

	// Poll
	fmt.Println("Polling...")
	for {
		fetches := client.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			// fmt.Println(string(record.Value), "from an iterator!")
			chanName := fmt.Sprintf("%s-%d", record.Topic, record.Partition)
			// println(chanName)
			ch := chanMap[chanName]
			ch <- record
		}
	}
}

func setUpChannels(size int) map[string]chan *kgo.Record {
	chanMap := make(map[string]chan *kgo.Record)
	ch := make(chan *kgo.Record, size)
	chanMap["parallel-consumer-0"] = ch
	return chanMap
}

func execute(ch <-chan *kgo.Record, process worker, processedRecordsChan chan<- *kgo.Record) {
	go func() {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		for record := range ch {
			err := process(ctx, record)
			if err != nil {
				// dlq
				fmt.Errorf("failed to process record: %s", string(record.Value))
				continue
			}
			processedRecordsChan <- record
		}
	}()
}

func closeChannels(chanMap map[string]chan *kgo.Record) {
	for _, v := range chanMap {
		close(v)
	}
}

func commitRecords(ctx context.Context, client *kgo.Client, ch <-chan *kgo.Record) {
	fmt.Println("Offset committer")
	for record := range ch {
		fmt.Printf("\n commiting record: %s", string(record.Value))
		client.CommitRecords(ctx, record)
	}
}
