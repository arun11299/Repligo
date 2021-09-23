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
	"fmt"

	"repligo/pkg/logger"
)

// MongoServerConfig
type MongoServerConfig struct {
	Server           string
	Port             uint16
	ConnectTimeoSecs int32
}

// CollectionConfig
type CollectionConfig struct {
	ServerDetails  MongoServerConfig
	NodeName       string
	DatabaseName   string
	CollectionName string
}

// StreamType
type StreamType int

const (
	// Change stream relay happen only from locationOne -> location_2
	UNIDIRECTIONAL StreamType = iota
	// Change stream happens from both locations locationOne <-> location_2
	BIDIRECTIONAL
)

// StreamingPolicy
type StreamingPolicy struct {
	Type StreamType
}

// DriverContext
type DriverContext struct {
	streamLocOne *StreamLocation
	streamLocTwo *StreamLocation
	policy       StreamingPolicy
	log          *logger.Logger
	channels     [2]*streamChannel
}

// streamChannel
type streamChannel struct {
	producer *ChangeStreamListener
	consumer *ChangeStreamConsumer
}

func buildMongoURIString(serverDetails MongoServerConfig) string {
	return fmt.Sprintf("mongodb://%s:%d", serverDetails.Server, serverDetails.Port)
}

// NewDriver
func NewDriver(policy StreamingPolicy, locationOne CollectionConfig, locationTwo CollectionConfig, log *logger.Logger) (*DriverContext, error) {

	log.Debug("Creating new mongo driver instance")

	sessionOne, err := NewSession(buildMongoURIString(locationOne.ServerDetails), 10, log)
	if err != nil {
		return nil, err
	}

	sLocOne, err := NewStreamLocation(sessionOne, locationOne.DatabaseName, locationOne.CollectionName, locationOne.NodeName)
	if err != nil {
		return nil, err
	}

	sessionTwo, err := NewSession(buildMongoURIString(locationTwo.ServerDetails), 10, log)
	if err != nil {
		return nil, err
	}

	sLocTwo, err := NewStreamLocation(sessionTwo, locationTwo.DatabaseName, locationTwo.CollectionName, locationTwo.NodeName)
	if err != nil {
		return nil, err
	}

	return &DriverContext{
		streamLocOne: sLocOne,
		streamLocTwo: sLocTwo,
		policy:       policy,
		log:          log,
		channels:     [2]*streamChannel{nil, nil},
	}, nil

}

// SetupStreams
func (driver *DriverContext) SetupStreams(opts StreamOptions) error {

	switch driver.policy.Type {
	case UNIDIRECTIONAL:
		driver.log.Info("Setting up unidirectional replication for mongo", "source", driver.streamLocOne.Name(), "destination", driver.streamLocTwo.Name())
		sChan, err := driver.setupUnidirectionalChannel(driver.streamLocOne, driver.streamLocTwo, opts)
		if err != nil {
			return err
		}
		driver.channels[0] = sChan

	case BIDIRECTIONAL:
		driver.log.Info("Setting up bidirectional replication for mongo. Channel 1.", "source", driver.streamLocOne.Name(),
			"destination", driver.streamLocTwo.Name())
		sChanOne, err := driver.setupUnidirectionalChannel(driver.streamLocOne, driver.streamLocTwo, opts)
		if err != nil {
			return err
		}
		driver.channels[0] = sChanOne

		driver.log.Info("Setting up bidirectional replication for mongo. Channel 2", "source", driver.streamLocTwo.Name(),
			"destination", driver.streamLocOne.Name())
		sChanTwo, err := driver.setupUnidirectionalChannel(driver.streamLocTwo, driver.streamLocOne, opts)
		if err != nil {
			return err
		}
		driver.channels[1] = sChanTwo

	default:
		panic(fmt.Sprintf("Unknown policy type received %s", driver.policy.Type))
	}
	return nil
}

func (driver *DriverContext) Start() {

	driver.log.InfoWithFields("Start streaming", "policyType", driver.policy.Type)

	switch driver.policy.Type {
	case UNIDIRECTIONAL:
		driver.startUni()

	case BIDIRECTIONAL:
		// TODO
	}
}

func (driver *DriverContext) startUni() {

	streamChannel := driver.channels[0]
	if streamChannel == nil {
		panic("Stream channel cannot be nil for starting streaming")
	}

	var cerr error
	cerr, cstopCh, cwaitCh := streamChannel.consumer.Start()
	if cerr != nil {
		driver.log.ErrorWithFields("Failed to start consumer", "error", cerr.Error())
	}

	perr, pstopCh, pwaitCh := streamChannel.producer.Start()
	if perr != nil {
		driver.log.ErrorWithFields("Failed to start producer", "error", perr.Error())
	}

	driver.log.Info("Mongo streaming started")

	// Wait for producer/consumer to error out or stop the processing loop
	// TODO: Below code probably is not safe because there is a chance that both
	// select cases will get triggered in response to the action on the other.
	select {
	case <-pwaitCh:
		close(pstopCh)
		streamChannel.consumer.Stop()

		if streamChannel.producer.streamError != nil {
			driver.log.ErrorWithFields("Error in producer", "error", streamChannel.producer.streamError.Error())
		}

	case <-cwaitCh:
		close(cstopCh)
		streamChannel.producer.Stop()

		if streamChannel.consumer.streamError != nil {
			driver.log.ErrorWithFields("Error in consumer", "error", streamChannel.consumer.streamError.Error())
		}
	}

	// TODO: Restart the flow OR just exit container ? Restart should handle it properly ?
}

func (driver *DriverContext) setupUnidirectionalChannel(
	sourceStreamLoc *StreamLocation, destStreamLoc *StreamLocation, opts StreamOptions) (*streamChannel, error) {

	consumeCh := make(chan ChangeStreamEvent)
	consumer, err := NewChangeStreamConsumer(destStreamLoc, sourceStreamLoc, consumeCh, opts)
	if err != nil {
		return nil, err
	}

	producer, err := NewChangeStreamListener(sourceStreamLoc, consumeCh, opts)
	if err != nil {
		return nil, err
	}

	return &streamChannel{
		producer: producer,
		consumer: consumer,
	}, nil
}
