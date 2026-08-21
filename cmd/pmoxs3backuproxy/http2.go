package main

import (
	"net"
	"time"
	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
	"tizbac/pmoxs3backuproxy/internal/s3pmoxcommon"

	"github.com/juju/clock"
	"github.com/juju/mutex/v2"
	"golang.org/x/net/http2"
)

var acquireProcessMutex = mutex.Acquire

func dataStoreLockName(endpoint string, datastore string) string {
	return s3pmoxcommon.DataStoreLockName(endpoint, datastore)
}

// beginDataStoreSession holds one inter-process lock per endpoint/bucket, with
// an in-process reference count for concurrent sessions on the same target.
// A single global counter is insufficient when one proxy serves two buckets:
// the second bucket would otherwise never acquire its own GC lock.
func (s *Server) beginDataStoreSession(endpoint string, datastore string) (string, error) {
	lockName := dataStoreLockName(endpoint, datastore)
	s.SessionsMutex.Lock()
	defer s.SessionsMutex.Unlock()
	if s.SessionLocks == nil {
		s.SessionLocks = make(map[string]*sessionLock)
	}
	entry := s.SessionLocks[lockName]
	if entry == nil {
		releaser, err := acquireProcessMutex(mutex.Spec{
			Clock: clock.WallClock, Name: lockName, Delay: time.Millisecond, Timeout: 30 * time.Second,
		})
		if err != nil {
			return "", err
		}
		entry = &sessionLock{releaser: releaser}
		s.SessionLocks[lockName] = entry
		s3backuplog.DebugPrint("Locked %s", lockName)
	}
	entry.count++
	s.Sessions++
	return lockName, nil
}

func (s *Server) endDataStoreSession(lockName string) {
	s.SessionsMutex.Lock()
	defer s.SessionsMutex.Unlock()
	entry := s.SessionLocks[lockName]
	if entry == nil || entry.count == 0 {
		s3backuplog.ErrorPrint("Attempted to release unknown datastore lock %s", lockName)
		return
	}
	entry.count--
	s.Sessions--
	if entry.count == 0 {
		entry.releaser.Release()
		delete(s.SessionLocks, lockName)
	}
}

func (s *Server) activeSessions() uint64 {
	s.SessionsMutex.Lock()
	defer s.SessionsMutex.Unlock()
	return s.Sessions
}

func (s *Server) backup(sock net.Conn, C TicketEntry, ds string, S s3pmoxcommon.Snapshot) {
	started := time.Now()
	lockName, err := s.beginDataStoreSession(C.Endpoint, ds)
	if err != nil {
		sock.Close()
		s3backuplog.ErrorPrint("Failed to acquire backup lock for %s: %s", ds, err)
		return
	}
	defer s.endDataStoreSession(lockName)

	srv := &http2.Server{}
	//We serve the HTTP2 connection back using default handler after protocol upgrade
	snew := &Server{
		H2Ticket:          &C,
		SelectedDataStore: &ds,
		Snapshot:          &S,
		Writers:           make(map[int32]*Writer),
		Finished:          false,
		ChunkS3Timeout:    s.ChunkS3Timeout,
	}
	srv.ServeConn(sock, &http2.ServeConnOpts{Handler: snew})
	if !snew.Finished { //Incomplete backup because connection died pve side, remove from S3
		s3backuplog.WarnPrint(
			"backup session incomplete remote=%s datastore=%s backup_id=%s duration=%s",
			sock.RemoteAddr(),
			ds,
			S.BackupID,
			time.Since(started).Round(time.Millisecond),
		)
		S.Datastore = ds
		if err := S.Delete(*C.Client); err != nil {
			s3backuplog.ErrorPrint("Failed to remove incomplete backup: " + err.Error())
		} else {
			s3backuplog.WarnPrint("Removed incomplete backup %s", snew.Snapshot.S3Prefix())
		}
	}
	s3backuplog.InfoPrint(
		"backup session finished remote=%s datastore=%s backup_id=%s duration=%s complete=%t",
		sock.RemoteAddr().String(),
		ds,
		S.BackupID,
		time.Since(started).Round(time.Millisecond),
		snew.Finished,
	)
}

func (s *Server) restore(sock net.Conn, C TicketEntry, ds string, S s3pmoxcommon.Snapshot) {
	started := time.Now()
	lockName, err := s.beginDataStoreSession(C.Endpoint, ds)
	if err != nil {
		sock.Close()
		s3backuplog.ErrorPrint("Failed to acquire restore lock for %s: %s", ds, err)
		return
	}
	defer s.endDataStoreSession(lockName)

	srv := &http2.Server{}
	//We serve the HTTP2 connection back using default handler after protocol upgrade
	snew := &Server{
		H2Ticket:          &C,
		SelectedDataStore: &ds,
		Snapshot:          &S,
		Writers:           make(map[int32]*Writer),
		Finished:          false,
	}
	srv.ServeConn(sock, &http2.ServeConnOpts{Handler: snew})

	s3backuplog.InfoPrint(
		"restore session finished remote=%s datastore=%s backup_id=%s duration=%s",
		sock.RemoteAddr().String(),
		ds,
		S.BackupID,
		time.Since(started).Round(time.Millisecond),
	)
}
