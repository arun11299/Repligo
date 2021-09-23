/*
Copyright 2021 Arun Muralidharan

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

package mongo

import (
	"context"
	"errors"
	"time"

	"repligo/pkg/logger"
	"repligo/pkg/stream"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ChangeStreamListener
// Implements the stream.StreamProducerConsumerInterface interface.
// Holds the context information associated with the change stream instance and loop.
type ChangeStreamListener struct {
	streamLoc   string
	stopCh      chan struct{}          // Channel to stop the producer from generating more events
	waitCh      chan struct{}          // wait for producer to finish the loop and come out of it
	consumerCh  chan ChangeStreamEvent // Consumer channel to which the events needs to be sent.
	streamError error                  // Error due to which stream exited
	eventStream *mongo.ChangeStream    // Change stream instance
	log         *logger.Logger
}

// ChangeStreamConsumer
// Implements the stream.StreamProducerConsumerInterface interface.
type ChangeStreamConsumer struct {
	streamLoc   *StreamLocation        // Location to which the consumer will operate on
	producerLoc *StreamLocation        // Producer location for writing the checkpoint result
	stopCh      chan struct{}          // Channel to stop the producer from generating more events
	waitCh      chan struct{}          // wait for producer to finish the loop and come out of it
	consumerCh  chan ChangeStreamEvent // Consumer channel to which the events needs to be sent.
	opts        StreamOptions
	streamError error // Error due to which consumer exited
	log         *logger.Logger
}

// ChangeStreamEvent
type ChangeStreamEvent struct {
	event       bson.M
	resumeToken bson.Raw
}

// NewChangeStreamListener
// Creates new instance of the change stream listener which is also the producer of the events.
// If resumeToken was set and the function throws an error, that means that we can no longer continue
// syncing from the last checkpoint. Need to either full sync the collection or continue from the latest change stream
// event based on the sync policy set, which the driver code should take care of.
func NewChangeStreamListener(sloc *StreamLocation, consumerCh chan ChangeStreamEvent, opts StreamOptions) (*ChangeStreamListener, error) {

	sloc.log.DebugWithFields("Creating new change stream listener", "streamLocation", sloc.Name())

	var csopts *options.ChangeStreamOptions

	if len(sloc.resumeToken) > 0 {
		csopts = &options.ChangeStreamOptions{
			BatchSize:   &opts.BatchSize,
			ResumeAfter: sloc.resumeToken,
		}
	} else {
		csopts = &options.ChangeStreamOptions{
			BatchSize: &opts.BatchSize,
		}
	}

	// start watching the changestream
	// eventStream is not goroutine safe. Must be accessed from only single place
	eventStream, err := sloc.C.Watch(context.TODO(), mongo.Pipeline{}, csopts)
	if err != nil {
		return nil, err
	}

	return &ChangeStreamListener{
		streamLoc:   sloc.Name(),
		stopCh:      make(chan struct{}),
		waitCh:      make(chan struct{}),
		consumerCh:  consumerCh,
		eventStream: eventStream,
		streamError: nil,
		log:         sloc.log,
	}, nil
}

// Start change stream listener
// Change listener loop is run on a separate goroutine spawned by this function.
// Returns channels to the client caller using which it either wait for stream to exit
// or stop it.
// It is expected the client to call this function from a separate goroutine different from main
// goroutine as the client is expected to wait/block till the loop finishes (which is never in ideal case)
func (producer *ChangeStreamListener) Start() (error, chan struct{}, chan struct{}) {

	producer.log.DebugWithFields("Change stream listener started", "streamLocation", producer.streamLoc)
	go producer.runListener()
	return nil, producer.stopCh, producer.waitCh
}

// Stop the change stream listener
// First the stop channel is closed to signal the gorouine running the stream loop.
// Then it waits for the wait channel to unblock which signals that the cleanup has finished.
// Returns any recorded error
func (producer *ChangeStreamListener) Stop() error {
	producer.log.DebugWithFields("Got request to stop change stream listener", "streamLocation", producer.streamLoc,
		"stream_error", producer.streamError)

	close(producer.stopCh)
	<-producer.waitCh

	producer.log.InfoWithFields("Change stream listener stopped", "streamLocation", producer.streamLoc)

	return producer.streamError
}

// Returns the error recorded if any during the change stream loop
func (producer *ChangeStreamListener) Error() error {
	return producer.streamError
}

func (producer *ChangeStreamListener) Stats() stream.StreamStats {
	return stream.StreamStats{}
}

// Runs the actual listener loop. Usually called in a separate goroutine.
// Listens for any signal on the stop channel in which case it breaks from
// the loop.
// When the producer loop quits with an error:
// 1. Stop the consumer loop.
// 2. Wait for consumer loop to finish.
// 3. Cleanup the stream locations. both producing and consuming.
// 4. Create new stream locations and ChangeStreamListener instance.
// 5. Rerun the producer and consumer loops.
// This way we can make sure that the change stream should start asking
// the data from its last commit or checkpoint and no data gets lost.
//
// In case the oplog history hast lost the resumeToken event, then a full
// transfer needs to be done based on the sync policy.
func (producer *ChangeStreamListener) runListener() {
	// close the change stream whenever we go out of scope
	defer producer.eventStream.Close(context.TODO())
	// Close the wait channel so that the waiting client goroutine can resume
	defer close(producer.waitCh)

	// Iterate the change stream and copy out each event until the change
	// stream is closed by the server or there is an error getting the next
	// event.
	for {
		select {
		case _ = <-producer.stopCh:
			producer.log.InfoWithFields("Got notification to stop producer loop", "streamLocation", producer.streamLoc)
			break

		default:
			if producer.eventStream.TryNext(context.TODO()) {
				// A new event variable should be declared for each event.
				var event bson.M
				if err := producer.eventStream.Decode(&event); err != nil {
					producer.streamError = err
					break
				}

				csEvent := ChangeStreamEvent{
					event:       event,
					resumeToken: producer.eventStream.ResumeToken(),
				}
				// Send it downstream to consumer
				producer.consumerCh <- csEvent
				continue
			}

			// If TryNext returns false, the next change is not yet available, the
			// change stream was closed by the server, or an error occurred. TryNext
			// should only be called again for the empty batch case.
			if err := producer.eventStream.Err(); err != nil {
				producer.streamError = err
				break
			}
			if producer.eventStream.ID() == 0 {
				producer.streamError = errors.New("Change stream closed due to error condition")
				break
			}
		}
	}
	return
}

// NewChangeStreamConsumer
// Creates an instance of ChangeStreamConsumer which implements producer-consumer interface.
// Consumer takes its consume channel as parameter instead of hiding it because it makes the interaction
// with producer (which also needs a reference to the consumer channel) easier to work with other interfaces like mock.
// otherwise, we will need to hide it behind an interface and do the down cast which seems to be less elegant.
func NewChangeStreamConsumer(sloc *StreamLocation, producerLoc *StreamLocation, consumerCh chan ChangeStreamEvent,
	opts StreamOptions) (*ChangeStreamConsumer, error) {

	return &ChangeStreamConsumer{
		streamLoc:   sloc,
		producerLoc: producerLoc,
		stopCh:      make(chan struct{}),
		waitCh:      make(chan struct{}),
		consumerCh:  consumerCh,
		opts:        opts,
		streamError: nil,
		log:         sloc.log,
	}, nil
}

// Start
// Interface method for stream.StreamProducerConsumerInterface.
// Starts a consumer on a different goroutine and returns a set of channels to control and wait
// for the consumer to do its job.
func (consumer *ChangeStreamConsumer) Start() (error, chan struct{}, chan struct{}) {

	consumer.log.DebugWithFields("Change stream listener started", "streamLocation", consumer.streamLoc.Name())
	go consumer.runConsumer()
	return nil, consumer.stopCh, consumer.waitCh
}

// Stop
// Interface method for stream.StreamProducerConsumerInterface.
// Stops the consumer loop and waits for it to cleanup and exit.
func (consumer *ChangeStreamConsumer) Stop() error {
	consumer.log.DebugWithFields("Got request to stop change stream consumer", "streamLocation", consumer.streamLoc.Name(),
		"stream_error", consumer.streamError)

	close(consumer.stopCh)
	<-consumer.waitCh
	consumer.log.InfoWithFields("Change stream consumer stopped", "streamLocation", consumer.streamLoc.Name())

	return consumer.streamError
}

// Error
// Interface method for stream.StreamProducerConsumerInterface.
// Returns the error observed in the consumer loop
func (consumer *ChangeStreamConsumer) Error() error {
	return consumer.streamError
}

// Stats
// Interface method for stream.StreamProducerConsumerInterface.
func (consumer *ChangeStreamConsumer) Stats() stream.StreamStats {
	return stream.StreamStats{}
}

// runConsumer
// Implements the actual consumer loop. It does the following:
// 1. Each event received is converted to a WriteModel and stored in a buffer.
// 2. On each ticker event, the stored buffered is bulk written to the stream location.
func (consumer *ChangeStreamConsumer) runConsumer() {

	// Close the wait channel to unblock the client goroutine
	defer close(consumer.waitCh)

	ticker := time.NewTicker(time.Duration(consumer.opts.ConsumerWaitPeriodSecs) * time.Second)
	defer ticker.Stop()

	models := make([]mongo.WriteModel, 0)
	var resumeToken bson.Raw

	for {
		select {
		case _ = <-consumer.stopCh:
			consumer.log.InfoWithFields("Received stop signal for consumer", "streamLocation", consumer.streamLoc.Name())
			break

		case csEvent := <-consumer.consumerCh:
			consumer.log.DebugWithFields("Received consumer event", "event", csEvent)

			err := consumer.processEvent(csEvent.event, &models)
			if err != nil {
				consumer.streamError = err
				break
			}
			// update the resume token
			resumeToken = csEvent.resumeToken

		case _ = <-ticker.C:
			if len(models) > 0 {
				consumer.bulkWrite(models, resumeToken)
				// clear the list, but keep the allocated memory
				models = models[:0]
			}
		}
	}
}

// This function is standalone and does not spawn any new goroutines inside it.
// Should always run from a single goroutine.
// For Changestream operations, refer: https://docs.mongodb.com/manual/reference/change-events/
// NOTE: Recovery works only with the provided oplog size. If the recovery time goes beyond that
// size, a manual mongodb sync needs to be done.
// For changing oplog size: https://docs.mongodb.com/manual/tutorial/change-oplog-size/
func (consumer *ChangeStreamConsumer) processEvent(event bson.M, models *[]mongo.WriteModel) error {

	switch event["operationType"] {
	case "insert":
		consumer.log.DebugWithFields("Received insert event", "event", event["operationType"],
			"streamLocation", consumer.streamLoc.Name())

		doc, err := bson.Marshal(event["fullDocument"])
		if err != nil {
			return err
		}
		model := &mongo.InsertOneModel{
			Document: doc,
		}
		*models = append(*models, model)
	}
	return nil
}

//bulkWriteWithRetries
// Does bulkWrite with retry option.
func (consumer *ChangeStreamConsumer) bulkWriteWithRetries(models []mongo.WriteModel, resumeToken bson.Raw) error {

	consumer.log.DebugWithFields("Writing the batched events to stream location with retries", "streamLocation", consumer.streamLoc.Name(),
		"length", len(models), "resumeToken", resumeToken)

	maxRetries := consumer.opts.MaxWriteRetries
	var currentRetryCount int32 = 0
	var lastErr error = nil

	for {
		if currentRetryCount >= maxRetries {
			consumer.log.ErrorWithFields("Failed to write bulk request since max number of retries exceeded",
				"streamLocation", consumer.streamLoc.Name(), "length", len(models), "resumeToken", resumeToken,
				"maxRetries", maxRetries, "error", lastErr.Error())
			return lastErr
		}

		err := consumer.bulkWrite(models, resumeToken)
		if err != nil {
			consumer.log.ErrorWithFields("Bulk write failed", "streamLocation", consumer.streamLoc.Name(), "error", err.Error())
			lastErr = err

			currentRetryCount += 1
			time.Sleep(time.Duration(consumer.opts.IntervalBetweenWriteRetriesSecs) * time.Second)
		}
		// No error. break from retry loop
		break
	}
	return nil
}

// bulkWrite
// Writes the batch to the stream location.
func (consumer *ChangeStreamConsumer) bulkWrite(models []mongo.WriteModel, resumeToken bson.Raw) error {

	consumer.log.DebugWithFields("Writing the batched events to stream location", "streamLocation", consumer.streamLoc.Name(),
		"length", len(models), "resumeToken", resumeToken)

	err := consumer.streamLoc.Write(models)
	if err != nil {
		return err
	}
	err = consumer.producerLoc.CheckpointResult(resumeToken)
	if err != nil {
		return err
	}
	return nil
}
