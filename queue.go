package fastq

import (
	"encoding/binary"
	"sync/atomic"
	"unsafe"
)

type Error struct {
	Value int
	msg   string
}

func (err Error) Error() string {
	return err.msg
}

var (
	ErrorRetryError = &Error{
		Value: -1,
		msg:   "Retry Error",
	}

	ErrorQueryFull = &Error{
		Value: -2,
		msg:   "Query full",
	}

	ErrorQueryEmpty = &Error{
		Value: -3,
		msg:   "query empty",
	}

	ErrorCasError = &Error{
		Value: -4,
		msg:   "cas error",
	}
)

const maxQueryLength = 1000

type Message struct {
	Module string
	Msg    string

	RetryTimes int
}

const maxMessageLength = 2000

type FastQueue struct {
	writeIndex int32
	readIndex  int32
	message    [maxQueryLength][maxMessageLength]byte
}

var query = &FastQueue{
	writeIndex: 0,
	readIndex:  0,
}

func Init() error {
	var key uintptr = 2
	var size uintptr = 4 + 4 + maxMessageLength*maxQueryLength
	shmid, err := Shmget(key, size, IPC_CREATE|0600)
	if err != 0 {
		return err
	}

	addr, err := Shmat(shmid)
	if err != 0 {
		return err
	}

	query = (*FastQueue)(unsafe.Pointer(uintptr(addr)))
	query.readIndex = 0
	query.writeIndex = 0

	return nil
}

func Empty() bool {
	return query.empty()
}

func Full() bool {
	return query.full()
}

// Write push mesg to queue
func Write(message *Message) error {
	return query.write(message)
}

// Read mesg from queue
func Read() (module string, message string, err error) {
	return query.read()
}

func (q *FastQueue) empty() bool {
	writeIndex := atomic.LoadInt32(&q.writeIndex)
	readIndex := atomic.LoadInt32(&q.readIndex)
	return writeIndex == readIndex
}

func (q *FastQueue) full() bool {
	writeIndex := atomic.LoadInt32(&q.writeIndex)
	readIndex := atomic.LoadInt32(&q.readIndex)
	nextIndex := (writeIndex + maxQueryLength + 1) % maxQueryLength
	return nextIndex == readIndex
}

func (q *FastQueue) write(message *Message) error {
	if message.RetryTimes < 0 {
		return ErrorRetryError
	}

	writeIndex := atomic.LoadInt32(&q.writeIndex)
	readIndex := atomic.LoadInt32(&q.readIndex)

	index := (writeIndex + maxQueryLength + 1) % maxQueryLength
	if index == readIndex {
		// queue full
		return ErrorQueryFull
	}

	if !atomic.CompareAndSwapInt32(&q.writeIndex, writeIndex, index) {
		message.RetryTimes--
		return q.write(message)
	}

	moduleLen := uint16(len(message.Module))
	msgLen := uint16(len(message.Msg))

	if 2+2+moduleLen+msgLen > maxMessageLength {
		message.Msg = message.Msg[:maxMessageLength-moduleLen-4]
		msgLen = uint16(len(message.Msg))
	}

	// 写入格式为：
	// 2字节module长度+2字节msg长度+module+msg

	binary.LittleEndian.PutUint16(q.message[writeIndex][0:2], moduleLen)
	binary.LittleEndian.PutUint16(q.message[writeIndex][2:4], msgLen)
	copy(q.message[writeIndex][4:4+moduleLen], message.Module)
	copy(q.message[writeIndex][4+moduleLen:], message.Msg)

	return nil

}

func (q *FastQueue) read() (module string, message string, err error) {
	writeIndex := atomic.LoadInt32(&q.writeIndex)
	readIndex := atomic.LoadInt32(&q.readIndex)

	if readIndex == writeIndex {
		err = ErrorQueryEmpty
		return
	}

	index := (readIndex + maxQueryLength + 1) % maxQueryLength
	if !atomic.CompareAndSwapInt32(&q.readIndex, readIndex, index) {
		err = ErrorCasError
		return
	}

	moduleLen := binary.LittleEndian.Uint16(q.message[readIndex][0:2])
	msgLen := binary.LittleEndian.Uint16(q.message[readIndex][2:4])
	module = string(q.message[readIndex][4 : 4+moduleLen])
	message = string(q.message[readIndex][4+moduleLen : 4+moduleLen+msgLen])

	return
}
