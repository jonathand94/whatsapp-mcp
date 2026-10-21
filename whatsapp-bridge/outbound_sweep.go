// outbound_sweep.go — freeing what drafts leave on disk.
//
// sends_media.go's header admits the gap this closes: a draft that is created
// and then abandoned kept its bytes until someone tried to confirm it, and
// nothing ever did. That was a leak of a few kilobytes per draft. With drafts
// of up to 2 GB, a handful of abandoned ones fill the disk the message
// database lives on, so they are now swept:
//
//   - at startup, before the API serves anything, every scratch and partial
//     file of the previous process: jobs live in memory, so after a restart
//     nothing can still be writing them — and nothing new can have started;
//   - at startup and every hour, every draft past its hour that nobody
//     confirmed. It is marked 'expired' — what confirm would have done on its
//     next attempt — and its bytes are deleted. A draft with an upload still
//     running is left alone: its confirm started in time.

package main

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"
)

// outboundSweepInterval is how often expired drafts are looked for.
const outboundSweepInterval = time.Hour

// sweepOutbound expires abandoned drafts and deletes their files.
func (s *Server) sweepOutbound(ctx context.Context) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT draft_id, COALESCE(content_file_path, '')
		FROM sends WHERE status = 'draft' AND created_at < ?`,
		time.Now().Unix()-draftTTLSeconds)
	if err != nil {
		log.Printf("outbound sweep: listing expired drafts failed: %v", err)
		return
	}
	type expired struct{ id, path string }
	var stale []expired
	for rows.Next() {
		var e expired
		if err := rows.Scan(&e.id, &e.path); err == nil {
			stale = append(stale, e)
		}
	}
	rows.Close()

	freed := 0
	for _, e := range stale {
		if up := s.jobs().get("send:" + e.id); up != nil && up.running() {
			continue
		}
		res, err := s.db.ExecContext(ctx,
			`UPDATE sends SET status = 'expired' WHERE draft_id = ? AND status = 'draft'`, e.id)
		if err != nil {
			log.Printf("outbound sweep: expiring draft %s failed: %v", e.id, err)
			continue
		}
		if n, _ := res.RowsAffected(); n == 0 {
			continue // confirmed in the meantime
		}
		if err := removeOutboundFile(e.path); err != nil {
			log.Printf("outbound sweep: removing %s failed: %v", e.path, err)
			continue
		}
		freed++
	}
	if freed > 0 {
		log.Printf("outbound sweep: expired %d abandoned draft(s) and deleted their files", freed)
	}
}

// removeOrphans deletes the scratch and partial files a previous process left:
// drafts still being prepared (.part), upload scratch (.upload-*.enc), voice
// note transcodes (.voice.ogg), and half-finished downloads in the media folder.
//
// Only safe before the API starts serving: once it does, a new draft's .part
// file is indistinguishable from an orphan.
func (s *Server) removeOrphans() {
	dir := outboundDir(s.cfg)
	patterns := []string{
		filepath.Join(dir, "*.part"),
		filepath.Join(dir, ".upload-*.enc"),
		filepath.Join(dir, "*.voice.ogg"),
		filepath.Join(s.cfg.MediaPath, "*.part"),
	}
	removed := 0
	for _, pattern := range patterns {
		matches, _ := filepath.Glob(pattern)
		for _, m := range matches {
			if err := os.Remove(m); err == nil {
				removed++
			}
		}
	}
	if removed > 0 {
		log.Printf("outbound sweep: removed %d file(s) left behind by the previous run", removed)
	}
}

// runOutboundSweeper sweeps now and then every interval until ctx ends.
func (s *Server) runOutboundSweeper(ctx context.Context) {
	s.sweepOutbound(ctx)
	t := time.NewTicker(outboundSweepInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sweepOutbound(ctx)
		}
	}
}
