package fastq

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmpty(t *testing.T) {
	q := &FastQueue{
		writeIndex: 0,
		readIndex:  0,
	}
	assert.True(t, q.empty(), "should be empty")
}

func TestFull(t *testing.T) {
	q := &FastQueue{
		writeIndex: 998,
		readIndex:  0,
	}
	assert.False(t, q.full(), "should not be full")
	q.writeIndex++
	assert.True(t, q.full(), "should be full")
}

func TestRetryError(t *testing.T) {
	q := &FastQueue{}
	msg := &Message{
		Module:     "test",
		Msg:        "TestRetryError",
		RetryTimes: -1,
	}
	assert.Equal(t, ErrorRetryError, q.write(msg), "retry error")
}

func TestWriteFullFailed(t *testing.T) {
	q := &FastQueue{
		writeIndex: 999,
	}
	msg := &Message{
		Module:     "test",
		Msg:        "TestWrite",
		RetryTimes: 0,
	}
	assert.Equal(t, ErrorQueryFull, q.write(msg), "write full should be failed")
}

func TestWrite(t *testing.T) {
	q := &FastQueue{}
	msg := &Message{
		Module:     "test",
		Msg:        "TestWrite",
		RetryTimes: 0,
	}
	assert.Nil(t, q.write(msg), "should write success")

	var message [maxMessageLength]byte
	binary.LittleEndian.PutUint16(message[0:2], 4)
	binary.LittleEndian.PutUint16(message[2:4], 9)
	copy(message[4:8], msg.Module)
	copy(message[8:], msg.Msg)
	assert.Equal(t, message, q.message[0], "message should be euqal")
	assert.Equal(t, int32(1), q.writeIndex, "writeIndex should be 1")
	assert.Equal(t, int32(0), q.readIndex, "readIndex should be 0")
}

func TestReadEmpty(t *testing.T) {
	q := &FastQueue{}
	module, message, err := q.read()
	assert.Equal(t, "", module, "module should be empty")
	assert.Equal(t, "", message, "message should be empty")
	assert.Equal(t, ErrorQueryEmpty, err, "err should be equal")
}

func TestRead(t *testing.T) {
	q := &FastQueue{}
	module := "test"
	message := "message"
	binary.LittleEndian.PutUint16(q.message[q.writeIndex][0:2], 4)
	binary.LittleEndian.PutUint16(q.message[q.writeIndex][2:4], 7)
	copy(q.message[q.writeIndex][4:8], module)
	copy(q.message[q.writeIndex][8:], message)

	q.writeIndex++

	actModule, actMessage, err := q.read()
	assert.Equal(t, module, actModule, "module should be equal")
	assert.Equal(t, message, actMessage, "message should be equal")
	assert.Nil(t, err, "err should be nil")
	assert.Equal(t, int32(1), q.readIndex, "")
	assert.Equal(t, int32(1), q.writeIndex, "")
}

func TestTrimMessage(t *testing.T) {
	q := FastQueue{}
	msg := &Message{
		Module: "test",
	}
	i := 0
	for i < 400 {
		msg.Msg += "hello"
		i++
	}
	assert.Nil(t, q.write(msg), "should write success")
	expect := ""
	i = 0
	for i < 398 {
		expect += "hello"
		i++
	}
	expect += "he"
	// assert.Equal(t, expect, string(q.message[0][:]), "should be equal")
	var message [maxMessageLength]byte
	binary.LittleEndian.PutUint16(message[0:2], 4)
	binary.LittleEndian.PutUint16(message[2:4], 1992)
	copy(message[4:8], msg.Module)
	copy(message[8:], expect)
	assert.Equal(t, message, q.message[0], "")
}
