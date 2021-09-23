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
	"time"

	"repligo/pkg/logger"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type Session struct {
	client  *mongo.Client
	context *context.Context
	log     *logger.Logger
}

func NewSession(uri string, contextTimeoSecs time.Duration, log *logger.Logger) (*Session, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}

	ctx, _ := context.WithTimeout(context.Background(), contextTimeoSecs)
	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	return &Session{
		client:  client,
		context: &ctx,
		log:     log,
	}, nil
}

func (session *Session) Client() *mongo.Client {
	return session.client
}

func (session *Session) Ping() error {
	return nil
}
