//go:build linux

package watcher

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/rs/zerolog/log"
	"github.com/tskawada/bisync/internal/changelog"
	"github.com/tskawada/bisync/internal/config"
	"golang.org/x/sys/unix"
)

var sizeOfEventMeta = uint32(unsafe.Sizeof(unix.FanotifyEventMetadata{}))

// SyncPairWatcher watches a single sync pair using fanotify.
type SyncPairWatcher struct {
	pair     config.SyncPairConfig
	nodeName string
	store    *changelog.Store
	filter   *Filter
	debounce *Debouncer
	selfPIDs sync.Map // map[int]struct{}: PIDs of our own rsync processes
}

// Watcher manages fanotify listeners for all sync pairs.
type Watcher struct {
	cfg      *config.Config
	store    *changelog.Store
	pairs    []*SyncPairWatcher
	notifyCh chan struct{}
}

// NewWatcher creates a Watcher for all configured sync pairs.
func NewWatcher(cfg *config.Config, store *changelog.Store, notifyCh chan struct{}) (*Watcher, error) {
	w := &Watcher{
		cfg:      cfg,
		store:    store,
		notifyCh: notifyCh,
	}
	for _, sp := range cfg.SyncPairs {
		filter, err := NewFilter(sp.Excludes)
		if err != nil {
			return nil, fmt.Errorf("compile excludes for %q: %w", sp.Name, err)
		}
		debounce := NewDebouncer(time.Duration(sp.DebounceSeconds) * time.Second)
		spw := &SyncPairWatcher{
			pair:     sp,
			nodeName: cfg.Node.Name,
			store:    store,
			filter:   filter,
			debounce: debounce,
		}
		w.pairs = append(w.pairs, spw)
	}
	return w, nil
}

// Start begins watching all sync pairs. It blocks until ctx is cancelled.
func (w *Watcher) Start(ctx context.Context) error {
	var wg sync.WaitGroup
	errCh := make(chan error, len(w.pairs))

	for _, spw := range w.pairs {
		wg.Add(1)
		go func(spw *SyncPairWatcher) {
			defer wg.Done()
			if err := spw.run(ctx, w.notifyCh); err != nil {
				errCh <- fmt.Errorf("pair %q: %w", spw.pair.Name, err)
			}
		}(spw)
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		return err
	}
	return nil
}

// FlushAll flushes all debounce buffers and drains pending events into changelog.
func (w *Watcher) FlushAll(ctx context.Context) {
	for _, spw := range w.pairs {
		spw.debounce.Flush()
		for {
			select {
			case ev := <-spw.debounce.Events():
				spw.writeToChangelog(ctx, ev)
			default:
				goto done
			}
		}
	done:
	}
}

// RegisterRsyncPID records a PID so its fanotify events are ignored.
func (w *Watcher) RegisterRsyncPID(pid int) {
	for _, spw := range w.pairs {
		spw.selfPIDs.Store(pid, struct{}{})
	}
}

// UnregisterRsyncPID removes a previously registered PID.
func (w *Watcher) UnregisterRsyncPID(pid int) {
	for _, spw := range w.pairs {
		spw.selfPIDs.Delete(pid)
	}
}

func (spw *SyncPairWatcher) run(ctx context.Context, notifyCh chan struct{}) error {
	// FAN_MARK_FILESYSTEM + FAN_REPORT_DIR_FID | FAN_REPORT_NAME:
	// watches the entire filesystem and provides directory path + filename in events.
	// This combination supports FAN_CREATE, FAN_DELETE, FAN_MOVED_* unlike FAN_MARK_MOUNT.
	faFd, err := unix.FanotifyInit(
		unix.FAN_CLASS_NOTIF|unix.FAN_CLOEXEC|unix.FAN_REPORT_DIR_FID|unix.FAN_REPORT_NAME,
		unix.O_RDONLY|unix.O_LARGEFILE|unix.O_CLOEXEC,
	)
	if err != nil {
		return fmt.Errorf("fanotify_init for %q: %w", spw.pair.LocalPath, err)
	}
	defer unix.Close(faFd)

	const watchMask = uint64(unix.FAN_CREATE | unix.FAN_DELETE | unix.FAN_CLOSE_WRITE |
		unix.FAN_MOVED_FROM | unix.FAN_MOVED_TO | unix.FAN_ATTRIB | unix.FAN_ONDIR)

	if err := unix.FanotifyMark(faFd,
		unix.FAN_MARK_ADD|unix.FAN_MARK_FILESYSTEM,
		watchMask,
		unix.AT_FDCWD,
		spw.pair.LocalPath,
	); err != nil {
		return fmt.Errorf("fanotify_mark for %q: %w", spw.pair.LocalPath, err)
	}

	log.Info().
		Str("pair", spw.pair.Name).
		Str("path", spw.pair.LocalPath).
		Msg("watcher: fanotify active")

	// Debounce consumer goroutine.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case ev := <-spw.debounce.Events():
				spw.writeToChangelog(ctx, ev)
				select {
				case notifyCh <- struct{}{}:
				default:
				}
			}
		}
	}()

	// Stopper pipe for clean shutdown without blocking on Poll.
	r, w, err := os.Pipe()
	if err != nil {
		return fmt.Errorf("stopper pipe: %w", err)
	}
	defer r.Close()
	defer w.Close()
	go func() {
		<-ctx.Done()
		w.Write([]byte("x")) //nolint:errcheck
	}()

	// mountFd is used by OpenByHandleAt to resolve file handles to paths.
	mountFd, err := unix.Open(spw.pair.LocalPath, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if err != nil {
		return fmt.Errorf("open sync dir: %w", err)
	}
	defer unix.Close(mountFd)

	return spw.pollLoop(faFd, int(r.Fd()), mountFd)
}

func (spw *SyncPairWatcher) pollLoop(faFd, stopFd, mountFd int) error {
	fds := [2]unix.PollFd{
		{Fd: int32(faFd), Events: unix.POLLIN},
		{Fd: int32(stopFd), Events: unix.POLLIN},
	}
	buf := make([]byte, 4096*int(sizeOfEventMeta))

	for {
		n, err := unix.Poll(fds[:], -1)
		if err == unix.EINTR || n == 0 {
			continue
		}
		if err != nil {
			return err
		}
		if fds[1].Revents&unix.POLLIN != 0 {
			return nil // context cancelled
		}
		if fds[0].Revents&unix.POLLIN != 0 {
			spw.readEvents(faFd, mountFd, buf)
		}
	}
}

func (spw *SyncPairWatcher) readEvents(faFd, mountFd int, buf []byte) {
	n, err := unix.Read(faFd, buf)
	if err != nil || n < int(sizeOfEventMeta) {
		return
	}
	i := 0
	for i+int(sizeOfEventMeta) <= n {
		meta := (*unix.FanotifyEventMetadata)(unsafe.Pointer(&buf[i]))
		if meta.Event_len < sizeOfEventMeta || i+int(meta.Event_len) > n {
			break
		}
		if meta.Vers == unix.FANOTIFY_METADATA_VERSION {
			spw.processEvent(meta, buf[i:i+int(meta.Event_len)], mountFd)
		}
		i += int(meta.Event_len)
	}
}

func (spw *SyncPairWatcher) processEvent(meta *unix.FanotifyEventMetadata, data []byte, mountFd int) {
	// With FAN_REPORT_DIR_FID|FAN_REPORT_NAME, Fd is always FAN_NOFD.
	// Path info comes from the FID info records appended after the metadata.
	if meta.Fd != unix.FAN_NOFD {
		unix.Close(int(meta.Fd)) //nolint:errcheck
		return
	}

	dirPath, fileName := resolveFIDPath(data, int(meta.Metadata_len), mountFd)
	if dirPath == "" {
		return
	}

	var fullPath string
	if fileName != "" {
		fullPath = filepath.Join(dirPath, fileName)
	} else {
		fullPath = dirPath
	}

	localBase := filepath.Clean(spw.pair.LocalPath)
	if !strings.HasPrefix(fullPath, localBase+string(os.PathSeparator)) && fullPath != localBase {
		return
	}

	relPath, err := filepath.Rel(localBase, fullPath)
	if err != nil || relPath == "." || relPath == "" {
		return
	}

	if spw.filter.Excluded(relPath) {
		return
	}

	if _, isSelf := spw.selfPIDs.Load(int(meta.Pid)); isSelf {
		return
	}
	// Exclude events from any rsync process (outgoing or incoming from peer).
	// This prevents the echo loop where an incoming rsync write triggers a
	// local changelog entry that then conflicts with subsequent remote ops.
	if isRsyncPID(meta.Pid) {
		return
	}

	isDir := meta.Mask&unix.FAN_ONDIR != 0
	evType := classifyMask(meta.Mask)
	if evType == "" {
		return
	}

	spw.debounce.Push(&RawEvent{
		SyncPair:  spw.pair.Name,
		Path:      relPath,
		EventType: evType,
		IsDir:     isDir,
		Pid:       int32(meta.Pid),
	})
}

// resolveFIDPath parses fanotify FID info records and resolves the directory path + filename.
// It looks for FAN_EVENT_INFO_TYPE_DFID_NAME or DFID records.
func resolveFIDPath(data []byte, metaLen int, mountFd int) (dirPath, fileName string) {
	off := metaLen
	for off+4 <= len(data) {
		infoType := data[off]
		// fanotify_event_info_header: InfoType(1) + pad(1) + Len(2)
		infoLen := int(binary.LittleEndian.Uint16(data[off+2 : off+4]))
		if infoLen == 0 || off+infoLen > len(data) {
			break
		}

		switch infoType {
		case unix.FAN_EVENT_INFO_TYPE_DFID_NAME, unix.FAN_EVENT_INFO_TYPE_DFID, unix.FAN_EVENT_INFO_TYPE_FID:
			// Layout after header(4 bytes): fsid(8 bytes) | handle_bytes(4) | handle_type(4) | handle_data(handle_bytes) | [filename\0]
			const headerSize = 4
			const fsidSize = 8
			hOff := off + headerSize + fsidSize
			if hOff+8 > off+infoLen {
				break
			}
			handleBytes := int(binary.LittleEndian.Uint32(data[hOff : hOff+4]))
			handleType := int32(binary.LittleEndian.Uint32(data[hOff+4 : hOff+8]))
			hDataOff := hOff + 8
			if hDataOff+handleBytes > off+infoLen {
				break
			}
			handle := unix.NewFileHandle(handleType, data[hDataOff:hDataOff+handleBytes])

			fd, errno := unix.OpenByHandleAt(mountFd, handle, unix.O_RDONLY|unix.O_PATH)
			if errno == nil {
				var nameBuf [unix.PathMax]byte
				n, _ := unix.Readlink(fmt.Sprintf("/proc/self/fd/%d", fd), nameBuf[:])
				if n > 0 {
					dirPath = string(nameBuf[:n])
				}
				unix.Close(fd) //nolint:errcheck
			}

			// For DFID_NAME, the filename follows the handle data.
			if infoType == unix.FAN_EVENT_INFO_TYPE_DFID_NAME {
				nameOff := hDataOff + handleBytes
				end := nameOff
				for end < off+infoLen && data[end] != 0 {
					end++
				}
				if end > nameOff {
					fileName = string(data[nameOff:end])
				}
			}

			if dirPath != "" {
				return dirPath, fileName
			}
		}

		off += infoLen
	}
	return "", ""
}

func isRsyncPID(pid int32) bool {
	comm, err := os.ReadFile(fmt.Sprintf("/proc/%d/comm", pid))
	if err != nil {
		return false
	}
	return strings.TrimSpace(string(comm)) == "rsync"
}

func classifyMask(mask uint64) string {
	switch {
	case mask&unix.FAN_DELETE != 0:
		return "delete"
	case mask&unix.FAN_CLOSE_WRITE != 0:
		return "modify"
	case mask&unix.FAN_CREATE != 0:
		return "create"
	// fanotify has no cookie to correlate MOVED_FROM↔MOVED_TO pairs,
	// so treat them as delete/create to ensure correct sync behavior.
	case mask&unix.FAN_MOVED_FROM != 0:
		return "delete"
	case mask&unix.FAN_MOVED_TO != 0:
		return "create"
	case mask&unix.FAN_ATTRIB != 0:
		return "attrib"
	}
	return ""
}

func (spw *SyncPairWatcher) writeToChangelog(ctx context.Context, ev debouncedEvent) {
	fullPath := filepath.Join(spw.pair.LocalPath, ev.Path)
	var size int64
	var mtime int64
	var mode uint32

	if info, err := os.Lstat(fullPath); err == nil {
		size = info.Size()
		mtime = info.ModTime().Unix()
		mode = uint32(info.Mode())
	}

	e := &changelog.Entry{
		SyncPair:  ev.SyncPair,
		Path:      ev.Path,
		EventType: ev.EventType,
		RenameTo:  ev.RenameTo,
		FileSize:  size,
		FileMtime: mtime,
		FileMode:  mode,
		IsDir:     ev.IsDir,
		NodeName:  spw.nodeName,
	}
	if _, err := spw.store.Append(ctx, e); err != nil {
		log.Error().Err(err).Str("path", ev.Path).Msg("changelog append failed")
	} else {
		log.Debug().
			Str("pair", ev.SyncPair).
			Str("path", ev.Path).
			Str("type", ev.EventType).
			Bool("isDir", ev.IsDir).
			Msg("changelog entry written")
	}
}
