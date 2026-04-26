# bisync

This is a bidirectional file synchronization tool for two peers.  
It uses inotify to watch for filesystem changes and rsync over SSH to transfer files. It is designed for simplicity, reliability, and performance.