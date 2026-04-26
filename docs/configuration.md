# Configuration Reference

bisync reads a single YAML file, typically `/etc/bisync/config.yaml`. The path can be overridden with `--config`. All fields have defaults except where noted as required.

## node

```yaml
node:
  name: "albus"   # required; must be unique across the two peers
```

`name` is an arbitrary string that identifies this node in changelog entries, conflict filenames, and log messages. It must differ from `peer.name`.

## peer

```yaml
peer:
  name: "tina"                       # required
  address: "tina"                    # required; Tailscale hostname or IP
  ssh_port: 22
  ssh_user: "root"
  ssh_key: "/root/.ssh/id_ed25519"
  grpc_port: 50051
```

`address` is the hostname or IP address used for both rsync/SSH and gRPC connections. `ssh_user` defaults to the `USER` environment variable, falling back to `bisync`. `ssh_key` defaults to `~/.ssh/id_ed25519`.

## sync_pairs

```yaml
sync_pairs:
  - name: "main"                    # required; must be unique within the config
    local_path: "/srv/csync2/main"  # required; must be an existing directory
    remote_path: "/srv/csync2/main" # path on the peer
    excludes:
      - "*.tmp"
      - ".Trash-*/**"
      - "2023/**/*.ARW"
    debounce_seconds: 5             # overrides watcher.debounce_seconds for this pair
```

`excludes` patterns follow the same glob syntax as `.gitignore`: `*` matches within a directory component, `**` matches across directory separators. Patterns are matched against the relative path from `local_path`.

`local_path` directories cannot overlap with each other. bisync validates this at startup.

## watcher

```yaml
watcher:
  debounce_seconds: 5   # how long to wait after the last event before flushing
  exclude_self: true    # ignored in current implementation (self-exclusion is always on)
```

`debounce_seconds` applies to all sync pairs unless overridden per pair. A lower value reduces latency; a higher value reduces the number of rsync invocations for rapidly changing files.

## transfer

```yaml
transfer:
  max_concurrent: 4       # maximum parallel rsync processes
  timeout_seconds: 864000 # per-rsync timeout (10 days; covers very large transfers)
  rsync_options:
    - "--compress"
    - "--partial"
    - "--partial-dir=.bisync-partial"
  bandwidth_limit: 0      # KB/s; 0 means unlimited
```

`rsync_options` are appended to the fixed rsync arguments (`-avz --dirs --files-from --partial --partial-dir`). Avoid duplicating flags that are already set by bisync. `bandwidth_limit` maps to rsync's `--bwlimit`.

## conflict

```yaml
conflict:
  policy: "lww"                  # lww | alpha_wins | keep_both | manual
  conflict_dir: ".bisync-conflicts"
  tombstone_retention_days: 30
```

`policy` determines how simultaneous edits on both nodes are resolved. See [overview.md](overview.md) for a description of each policy.

`conflict_dir` is a path relative to `local_path` where the losing file is moved when policy is `keep_both`.

`tombstone_retention_days` controls how long delete entries are kept in the changelog after they are synced. Setting this too low risks a deleted file being re-created if the peer reconciles after the tombstone has been pruned.

## recovery

```yaml
recovery:
  catchup_scan: true
  catchup_workers: 4
```

When `catchup_scan` is true, bisync scans sync pair directories on startup and records `modify` entries for files changed since the last clean shutdown. `catchup_workers` is the number of goroutines used during the walk.

## changelog

```yaml
changelog:
  db_path: "/var/lib/bisync/changelog.db"
  wal_mode: true
  retention_days: 90
```

`db_path` must be writable by the daemon user. The directory is not created automatically; ensure it exists before starting. `retention_days` controls how long synced entries are retained before pruning.

## notify

```yaml
notify:
  handlers:
    - type: "webhook"
      url: "https://example.com/hook"
      events: ["conflict", "error"]
    - type: "command"
      command: "/usr/local/bin/notify-admin"
      events: ["conflict"]
```

Each handler fires on the listed event types. `conflict` fires when a conflict is detected; `error` fires on transfer failures. The `webhook` type sends a JSON POST; `command` execs the script with event details in environment variables. Both types are optional; omit the `notify` section entirely to disable notifications.

## logging

```yaml
logging:
  level: "info"          # debug | info | warn | error
  output: "stdout"       # stdout | file
  file_path: "/var/log/bisync/bisync.log"
  max_size_mb: 1000
  max_backups: 5
```

When running under systemd, `stdout` output is captured by journald and `file_path` is unused unless `output: "file"` is set. Log rotation (`max_size_mb`, `max_backups`) applies only to file output.

`debug` level logs every filesystem event, every changelog write, and the result of each reconcile cycle. It is useful for troubleshooting but produces significant volume on active directories.
