package fastq

import (
	"syscall"
)

const (
	// IPC_CREATE create if key is nonexistent
	IPC_CREATE = 00001000
)

// Shmget 创建shmid
func Shmget(key, size, shmflg uintptr) (shmid uintptr, err syscall.Errno) {
	shmid, _, err = syscall.Syscall(syscall.SYS_SHMGET, key, size, shmflg)
	return
}

// Shmat attach到shmid的地址
func Shmat(shmid uintptr) (addr uintptr, err syscall.Errno) {
	addr, _, err = syscall.Syscall(syscall.SYS_SHMAT, shmid, 0, 0)
	return
}

// Shmdt deattach shmid
func Shmdt(shmid uintptr) syscall.Errno {
	_, _, err := syscall.Syscall(syscall.SYS_SHMDT, shmid, 0, 0)
	return err
}
