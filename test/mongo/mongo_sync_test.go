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

package mongo_test

import (
	"context"
	"testing"
	"time"

	"repligo/pkg/logger"
	"repligo/pkg/mongo"

	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func configureLogging() *logger.Logger {
	ctx := make(map[string]interface{})
	ctx["worker"] = "repligo-mongo"

	log := logger.NewConsoleLogger(ctx, true)
	return log
}

func TestChangeUniStreamOnlineCopy(t *testing.T) {
	log := configureLogging()

	policy := mongo.StreamingPolicy{mongo.UNIDIRECTIONAL}
	serverDetails := mongo.MongoServerConfig{
		Server:           "mongodb",
		Port:             27017,
		ConnectTimeoSecs: 10,
	}

	coll1 := mongo.CollectionConfig{
		ServerDetails:  serverDetails,
		NodeName:       "lp1",
		DatabaseName:   "db1",
		CollectionName: "db1",
	}
	coll2 := mongo.CollectionConfig{
		ServerDetails:  serverDetails,
		NodeName:       "lp2",
		DatabaseName:   "db2",
		CollectionName: "db2",
	}

	driver, err := mongo.NewDriver(policy, coll1, coll2, log)
	assert.Equal(t, nil, err)

	opts := mongo.StreamOptions{
		BatchSize:                       10,
		ConsumerWaitPeriodSecs:          1,
		MaxWriteRetries:                 3,
		IntervalBetweenWriteRetriesSecs: 5,
	}

	err = driver.SetupStreams(opts)
	assert.Equal(t, nil, err)

	// Start of the producer-consumer in another goroutine
	go driver.Start()

	// Add some events to collection db1 and see if it gets synced to db2
	sess, err := mongo.NewSession("mongodb://mongodb:27017/", 10, log)
	assert.Equal(t, nil, err)

	db1 := sess.Client().Database("db1")
	db1Coll := db1.Collection("db1")

	db2 := sess.Client().Database("db2")
	db2Coll := db2.Collection("db2")

	// First assert that num records in collection are zero initially
	countOpts := options.Count().SetMaxTime(2 * time.Second)
	count1, err := db1Coll.CountDocuments(
		context.TODO(),
		nil,
		countOpts)

	assert.Equal(t, "document is nil", err.Error())

	count2, err := db2Coll.CountDocuments(
		context.TODO(),
		bson.M{},
		countOpts,
	)
	assert.Equal(t, err, nil)

	assert.Equal(t, count1, count2)

	for i := 0; i < 10; i++ {
		_, err = db1Coll.InsertOne(context.TODO(), bson.D{{"value", i}})
		assert.Equal(t, err, nil)
	}

	// Wait for enough time for records to get sync.
	// The wait time is set to 1 sec in the consumer loop. So,
	// sleep for 5 seconds which should be more than enough for the records to sync
	time.Sleep(time.Duration(5) * time.Second)

	count2, err = db2Coll.CountDocuments(
		context.TODO(),
		bson.M{},
		countOpts,
	)
	assert.Equal(t, nil, err)

	var expectedCount int64 = 10

	assert.Equal(t, expectedCount, count2)
}
