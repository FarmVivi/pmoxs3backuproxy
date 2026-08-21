package main

import (
	"os"
	"os/signal"
	"syscall"

	"tizbac/pmoxs3backuproxy/internal/s3backuplog"
)

func (s *Server) handleSignal() {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)

	go func() {
		var signalcnt int = 0
		for {
			sig := <-sigs
			switch sig {
			case syscall.SIGINT, os.Interrupt, syscall.SIGTERM:
				activeSessions := s.activeSessions()
				if activeSessions > 0 && signalcnt < 1 {
					s3backuplog.WarnPrint("%d sessions active skipping shutdown.", activeSessions)
					s3backuplog.WarnPrint("Send signal again to force exit.")
					signalcnt += 1
					continue
				}
				/**
				 * Asking the process to stop is not a failure. Exiting non
				 * zero here made every restart of the service look like a
				 * crash in the journal, which is worse than cosmetic: it
				 * makes a real crash indistinguishable from a deployment.
				 *
				 * Being forced out while sessions are still running is a
				 * different matter, as it aborts backups in flight, and
				 * keeps a failure status.
				 **/
				if activeSessions > 0 {
					s3backuplog.WarnPrint(
						"Received signal %d, forced exit with %d sessions still active",
						sig, activeSessions,
					)
					os.Exit(1)
				}
				s3backuplog.InfoPrint("Received signal %d, exiting", sig)
				os.Exit(0)
			}
		}
	}()
}
