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

// Logging wrapper

package logger

import (
	"fmt"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger A generic wrapper over any logging implementation.
type Logger struct {
	log *zap.SugaredLogger // zap logger instance
}

// NewConsoleLogger Creates a new logging instance with the provided
// constant fields
func NewConsoleLogger(fields map[string]interface{}, debugLog bool) *Logger {

	sevLevel := zap.NewAtomicLevelAt(zapcore.DebugLevel)
	if debugLog {
		sevLevel = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}

	outputPaths := []string{"stdout"}
	errorPaths := []string{"stderr"}

	cfg := zap.Config{
		Encoding:         "json",
		Level:            sevLevel,
		OutputPaths:      outputPaths,
		ErrorOutputPaths: errorPaths,

		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:  "message",
			LevelKey:    "level",
			TimeKey:     "time",
			EncodeLevel: zapcore.CapitalLevelEncoder,
			EncodeTime:  zapcore.ISO8601TimeEncoder,
		},
	}

	logger, err := cfg.Build()
	if err != nil {
		panic(fmt.Sprintf("Error creation logger: %s", err.Error()))
	}

	// Add the constant fields
	zfields := make([]zap.Field, 0)

	for k, v := range fields {
		field := zap.Any(k, v)
		zfields = append(zfields, field)
	}
	logger = logger.With(zfields...)

	defer logger.Sync()
	sugar := logger.Sugar()

	return &Logger{
		log: sugar,
	}
}

// Sync Flushes any buffered log records.
func (elog *Logger) Sync() {
	elog.log.Sync()
}

// WithFields Creates a new logging instance with additional constant
// fields. The new fields are not added to the parent logging instance.
func (elog *Logger) WithFields(fields ...interface{}) *Logger {
	// Add the new fields
	return &Logger{
		log: elog.log.With(fields...),
	}
}

// Debug uses fmt.Sprint to construct and log a message.
func (elog *Logger) Debug(msg string, args ...interface{}) {
	elog.log.Debugf(msg, args...)
}

// DebugWithFields logs a message with some additional context. The variadic key-value pairs are treated
// as the context key values.
func (elog *Logger) DebugWithFields(msg string, keysAndValues ...interface{}) {
	elog.log.Debugw(msg, keysAndValues...)
}

// Error uses fmt.Sprintf to log a templated message.
func (elog *Logger) Error(msg string, args ...interface{}) {
	elog.log.Errorf(msg, args...)
}

// ErrorWithFields logs a message with some additional context. The variadic key-value pairs are treated
// as the context key values.
func (elog *Logger) ErrorWithFields(msg string, keysAndValues ...interface{}) {
	elog.log.Errorw(msg, keysAndValues...)
}

// Fatal uses fmt.Sprint to construct and log a message, then calls os.Exit.
func (elog *Logger) Fatal(msg string, args ...interface{}) {
	elog.log.Fatalf(msg, args...)
}

// FatalWithFields logs a message with some additional contex and then calls os.Exit.
// The variadic key-value pairs are treated as the context key values.
func (elog *Logger) FatalWithFields(msg string, keysAndValues ...interface{}) {
	elog.log.Fatalw(msg, keysAndValues...)
}

// Info uses fmt.Sprintf to log a templated message.
func (elog *Logger) Info(msg string, args ...interface{}) {
	elog.log.Infof(msg, args...)
}

// InfoWithFields logs a message with some additional context. The variadic key-value pairs are treated
// as the context key values.
func (elog *Logger) InfoWithFields(msg string, keysAndValues ...interface{}) {
	elog.log.Infow(msg, keysAndValues...)
}

// Panic uses fmt.Sprint to construct and log a message, then panics.
func (elog *Logger) Panic(msg string, args ...interface{}) {
	elog.log.Panicf(msg, args...)
}

// PanicWithFields logs a message with some additional contex and then panics.
// The variadic key-value pairs are treated as the context key values.
func (elog *Logger) PanicWithFields(msg string, keysAndValues ...interface{}) {
	elog.log.Panicw(msg, keysAndValues...)
}

// Warn uses fmt.Sprint to construct and log a message.
func (elog *Logger) Warn(msg string, args ...interface{}) {
	elog.log.Warnf(msg, args...)
}

// WarnWithFields logs a message with some additional context. The variadic key-value pairs are treated
// as the context key values.
func (elog *Logger) WarnWithFields(msg string, keysAndValues ...interface{}) {
	elog.log.Warnw(msg, keysAndValues...)
}
