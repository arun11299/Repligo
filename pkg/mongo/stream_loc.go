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
	"fmt"
	"time"

	"repligo/pkg/logger"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	collectionName string = "cs_checkpoint" // The collection name that will be used for checkpoiting replication
)

// StreamLocation
// Represents a logical place from/to where the data will get replicated
// TODO: Needs to implement some interface for mocking.
type StreamLocation struct {
	databaseName   string
	collectionName string
	nodeName       string
	session        *Session
	D              *mongo.Database
	C              *mongo.Collection
	chkPoint       *mongo.Collection
	chkPointOid    *primitive.ObjectID // The checkpoint entry id
	resumeToken    bson.Raw            // The token from which to begin recovery
	log            *logger.Logger
}

type StampTime struct {
	EpochInSecs uint32
	Ordinal     uint32
}

// StreamOptions
type StreamOptions struct {
	BatchSize                       int32
	ConsumerWaitPeriodSecs          int32
	MaxWriteRetries                 int32
	IntervalBetweenWriteRetriesSecs int32
}

// Default stream options that can be used directly
var DefaultStreamOptions StreamOptions = StreamOptions{
	BatchSize:                       10,
	ConsumerWaitPeriodSecs:          10,
	MaxWriteRetries:                 3,
	IntervalBetweenWriteRetriesSecs: 5,
}

// CheckpointRecord
// The record schema for the checkpoint collection
type CheckpointRecord struct {
	ID          primitive.ObjectID `bson:"_id,omitempty"`
	CreatedDate time.Time          `json:"createdDate"`
	LastUpdate  *time.Time         `json:"lastUpdate,omitempty"`
	ResumeToken bson.Raw           `json:"resumeToken,omitempty"`
}

// updateCheckpointDetails
// Given a database, get or create a new collection for checkpointing the replication
// process.
func (sloc *StreamLocation) updateCheckpointDetails() error {

	coll := sloc.D.Collection(collectionName)

	if coll == nil {
		// Create new collection
		err := sloc.D.CreateCollection(context.TODO(), collectionName, nil)
		if err != nil {
			return err
		}
		coll = sloc.D.Collection(collectionName)

		sloc.log.DebugWithFields("Created checkpoint collection", "databaseName", sloc.D.Name(), "collectionName",
			coll.Name(), "checkPointCollectionName", collectionName)

	} else {
		// collection already exists, check if any checkpoint entry exists.
		// If checkpoint entry exists, cache its oid, else leave it as nil
		cursor, err := coll.Find(context.TODO(), bson.M{})
		if err != nil {
			return err
		}

		var entries []CheckpointRecord
		if err = cursor.All(context.TODO(), &entries); err != nil {
			return err
		}
		// We expect only one checkpoint entry
		// TODO: recover from failure. Get the latest entry ?
		if len(entries) > 1 {
			sloc.log.ErrorWithFields("Expected only one entry for checkpoint", "databaseName", sloc.D.Name(), "collectionName", coll.Name())
			return errors.New("more than one checkpoint entry")
		}

		// Get the oid of the record and update it in memory
		if len(entries) == 1 {
			sloc.chkPointOid = &entries[0].ID
			sloc.resumeToken = entries[0].ResumeToken

			sloc.log.InfoWithFields("Checkpoint information recovered", "databaseName", sloc.D.Name(), "collectionName", coll.Name(),
				"checkpointOid", sloc.chkPointOid, "resumeToken", sloc.resumeToken)
		}
	}

	sloc.chkPoint = coll
	return nil
}

// NewStreamLocation
// Creates a new stream location
func NewStreamLocation(S *Session, databaseName string,
	collectionName string, nodeName string) (*StreamLocation, error) {

	// TODO
	db := S.Client().Database(databaseName)
	if db == nil {
		return nil, errors.New("Database not created")
	}

	coll := db.Collection(collectionName)
	if coll == nil {
		return nil, errors.New("collection does not exist")
	}

	sloc := StreamLocation{
		databaseName:   databaseName,
		collectionName: collectionName,
		nodeName:       nodeName,
		session:        S,
		D:              db,
		C:              coll,
		chkPoint:       nil,
		chkPointOid:    nil,
		resumeToken:    bson.Raw{},
		log:            S.log,
	}

	err := sloc.updateCheckpointDetails()
	if err != nil {
		return nil, err
	}

	return &sloc, nil
}

func (sloc *StreamLocation) Name() string {
	return fmt.Sprintf("%s-%s-%s", sloc.nodeName, sloc.databaseName, sloc.collectionName)
}

// TODO: parameter not generic to make it an interface
// Write
// Persistes the information to the stream location and also adds
// the snapshot time to the checkpoint so that we can recover from that
// point onwards.
// How far in past from where we can recover or replay using the snapsot time
// depends upon the oplog size configuration.
//
// NOTE: We assume that this function is called from only a single goroutine
// at any point in time.
func (sloc *StreamLocation) Write(models []mongo.WriteModel) error {

	session, err := sloc.D.Client().StartSession(options.Session())
	if err != nil {
		sloc.log.ErrorWithFields("Failed to create write session", "databaseName", sloc.databaseName,
			"collectionName", sloc.collectionName, "nodeName", sloc.nodeName)
		return err
	}

	defer session.EndSession(context.TODO())

	// Get the bulk write result to be passed for stats
	var bulkWriteResult *mongo.BulkWriteResult = nil
	_ = bulkWriteResult

	err = mongo.WithSession(context.TODO(), session, func(sessionContext mongo.SessionContext) error {
		// Use sessionContext as the context parameter for BulkWrite
		var wrErr error
		bulkWriteResult, wrErr = sloc.C.BulkWrite(sessionContext, models)
		return wrErr
	})
	if err != nil {
		return err
	}
	return nil
}

// Parameter `resumeToken` is the "data" field in the change stream event
// for the last entry in the batch. It will be persisted into checkpoint on
// successful write operation.
func (sloc *StreamLocation) CheckpointResult(resumeToken bson.Raw) error {

	var record CheckpointRecord
	currTime := time.Now()

	if sloc.chkPointOid == nil {

		// We do not know the oid that means there is no checkpoint entry.
		// Create a checkpoint entry with the snapshot time and update
		// the in memory oid for the checkpoint record. We will use this for the
		// next updates.
		record = CheckpointRecord{
			CreatedDate: currTime,
			LastUpdate:  &currTime,
			ResumeToken: resumeToken,
		}

		res, err := sloc.chkPoint.InsertOne(context.TODO(), record)
		if err != nil {
			sloc.log.ErrorWithFields("Failed to insert entry to checkpoint table", "error", err.Error())
			return err
		}

		tmpoid := res.InsertedID.(primitive.ObjectID)
		sloc.chkPointOid = &tmpoid

	} else {
		// We already have an existing record id handle to the checkpoint data.
		// Update with the latest snapshot time
		record = CheckpointRecord{
			LastUpdate:  &currTime,
			ResumeToken: resumeToken,
		}

		opts := options.Update()
		filter := bson.D{{"_id", sloc.chkPointOid}}
		update := bson.M{"$set": record}

		_, err := sloc.chkPoint.UpdateOne(context.TODO(), filter, update, opts)
		if err != nil {
			sloc.log.ErrorWithFields("Failed to update checkpoint entry", "error", err.Error())
			return err
		}
	}

	return nil
}
