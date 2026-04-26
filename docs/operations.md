# Operations Guide

## Building

bisync requires Go 1.21 or later and `rsync` installed on the host.

```sh
make build          # linux/amd64  → bin/bisync
make build-arm64    # linux/arm64  → bin/bisync-arm64
make build-armv7    # linux/armv7  → bin/bisync-armv7
make build-all      # all three
```

## Deployment

Copy the binary and config to each host, then install as root. The example below assumes two nodes named `node-a` and `node-b`:

```sh
scp bin/bisync          user@node-a:/tmp/bisync
scp config/node-a.yaml  user@node-a:/tmp/config.yaml
scp deploy/bisync.service user@node-a:/tmp/bisync.service
```

On each host:

```sh
install -m 755 /tmp/bisync /usr/bin/bisync
mkdir -p /etc/bisync /var/lib/bisync /var/log/bisync
install -m 640 /tmp/config.yaml /etc/bisync/config.yaml
install -m 644 /tmp/bisync.service /etc/systemd/system/bisync.service
systemctl daemon-reload
systemctl enable --now bisync
```

Validate the config before starting:

```sh
bisync validate --config /etc/bisync/config.yaml
```

## First-run considerations

bisync does not perform an initial full sync. On first boot it skips the catchup scan (no `last_shutdown` record exists yet) and begins watching for new changes only. If the two nodes already have data that needs to be reconciled, run rsync manually in both directions before enabling bisync, or accept that any divergence at startup will only be corrected as files are subsequently modified.

## systemd management

```sh
systemctl status bisync
journalctl -u bisync -f          # follow live logs
journalctl -u bisync --since "1 hour ago"
systemctl restart bisync
systemctl stop bisync
```

The service runs as root because fanotify with `FAN_MARK_FILESYSTEM` requires `CAP_SYS_ADMIN`. The unit file applies `ProtectSystem=full` and restricts write access to the sync data directories and log directory.

## CLI reference

### `bisync daemon`

Start the sync daemon. This is what systemd runs. It can also be invoked as `bisync` with no subcommand.

```sh
bisync daemon --config /etc/bisync/config.yaml
bisync daemon --config /etc/bisync/config.yaml --dry-run
```

`--dry-run` logs what would be transferred without executing rsync or modifying files. Useful for verifying configuration.

### `bisync status`

Pings the peer gRPC server and prints its reported status.

```sh
bisync status --config /etc/bisync/config.yaml
# Peer "node-b": status=ok
```

This requires the peer daemon to be running. A connection failure indicates a network problem, a stopped peer daemon, or a firewall blocking port 50051.

### `bisync validate`

Loads and validates the config file, printing a summary if valid or an error if not.

```sh
bisync validate --config /etc/bisync/config.yaml
# Config OK: node=node-a peer=node-b sync_pairs=[main]
```

### `bisync log`

Prints unsynced changelog entries as JSON. Useful for inspecting what the daemon believes needs to be sent.

```sh
bisync log --config /etc/bisync/config.yaml
bisync log --config /etc/bisync/config.yaml --tail 20
```

### `bisync conflicts`

Lists unsynced entries. At present this includes all pending entries, not only those blocked by a manual conflict policy.

```sh
bisync conflicts --config /etc/bisync/config.yaml
```

### `bisync resolve`

Stub for manual conflict resolution. Not yet fully implemented; it prints a message but does not modify the changelog or files. Resolve manual conflicts by editing files directly and restarting the daemon.

```sh
bisync resolve --keep-local /path/to/file
bisync resolve --keep-remote /path/to/file
```

### `bisync version`

Prints the version string.

## Monitoring

bisync logs every reconcile cycle at debug level and significant events (conflicts, transfer errors, watcher errors) at warn or error level. Under normal operation the journal is quiet at `info` level.

Useful log patterns to grep for:

```
reconcile: done           — normal completion; shows local_synced and remote_acked counts
rsync: failed             — transfer error, will retry
conflict:                 — any conflict event
watcher: fatal error      — the daemon is about to stop due to a watcher failure
```

To check whether the daemon is picking up filesystem events, set `logging.level: "debug"` and watch for `changelog entry written` lines after making a change.

## Troubleshooting

**Changes are not being detected**

Verify that fanotify is available. bisync uses `FAN_MARK_FILESYSTEM`, which requires kernel 5.1 or later and `CAP_SYS_ADMIN`. Check `journalctl -u bisync` for `fanotify_init` errors at startup. Confirm that the sync directory is on a supported filesystem (ext4, xfs, btrfs, and most standard Linux filesystems are fine; network-mounted filesystems such as NFS or CIFS are not).

**Changes are detected but not transferred**

Check that rsync is installed and reachable on both nodes. Verify that the SSH key is in place and that the SSH connection works without password prompt:

```sh
ssh -i /root/.ssh/id_ed25519 -p 22 root@node-b echo ok
```

Check for rsync errors in the journal. A repeated `rsync: failed` at attempt 0 usually indicates an SSH or path problem.

**Both old and new names appear after a rename**

This is expected behaviour. Because fanotify cannot correlate move pairs, bisync treats a rename as delete + create. The old name generates a delete event, and the new name generates a create event. These are processed in two separate reconcile cycles, so there is a window where both names coexist on the peer. The old name is removed once the delete entry is processed.

**Sync is slow after a period of inactivity**

The first reconcile after events arrive fires within `debounce_seconds` of the last event. If the daemon was not running, the catchup scan on restart generates entries for all modified files, which are then transferred in the first reconcile cycle. Large catchup batches take time proportional to the data volume.

**The daemon starts but the peer is unreachable**

bisync does not wait for peer connectivity at startup. The reconciler will log errors on each failed cycle and retry on the next tick (every 30 s). Once the peer becomes reachable, the pending changelog entries will be processed automatically.

**Disk space on the SQLite database**

The changelog grows with every filesystem event and is pruned daily. If the sync pair is very active or `retention_days` is high, the database can grow large. To reduce it manually, stop the daemon and run:

```sh
sqlite3 /var/lib/bisync/changelog.db "DELETE FROM changelog WHERE synced=1 AND created_at < datetime('now', '-30 days'); VACUUM;"
```

Then restart the daemon.
