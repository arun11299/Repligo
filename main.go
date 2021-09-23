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

package main

import (
	"repligo/pkg/logger"
	"repligo/pkg/mongo"
)

// configureLogging Setup logging context
func configureLogging() *logger.Logger {
	ctx := make(map[string]interface{})
	ctx["worker"] = "repligo-mongo"

	log := logger.NewConsoleLogger(ctx, true)
	return log
}

func main() {
	log := configureLogging()

	sess, err := mongo.NewSession("mongodb://mongodb:27017/", 10, log)
	if err != nil {
		panic(err)
	}
	coll, err := mongo.NewStreamLocation(sess, "events", "events", "lp1")
	if err != nil {
		panic(err)
	}

	coll2, err := mongo.NewStreamLocation(sess, "testdb", "testdb", "lp1")
	if err != nil {
		panic(err)
	}

	consumeCh := make(chan mongo.ChangeStreamEvent)
	consumer, err := mongo.NewChangeStreamConsumer(coll2, coll, consumeCh, mongo.DefaultStreamOptions)
	if err != nil {
		panic(err)
	}
	cerr, cstopCh, cwaitCh := consumer.Start()
	if cerr != nil {
		panic(err)
	}

	producer, err := mongo.NewChangeStreamListener(coll, consumeCh, mongo.DefaultStreamOptions)
	if err != nil {
		panic(err)
	}
	perr, pstopCh, pwaitCh := producer.Start()
	if perr != nil {
		panic(err)
	}

	<-pwaitCh
	close(pstopCh)

	close(cstopCh)
	<-cwaitCh

}
