# bisync

This is a bidirectional file synchronization tool for two peers.  
It uses inotify to watch for filesystem changes and rsync over SSH to transfer files. It is designed for simplicity, reliability, and performance.

bisync is not meant as a replacement for tools like Syncthing or Unison. It aims to provide bidirectional synchronisation with minimal code, targeting small-scale, trusted-network setups such as homelabs.
The implementation is deliberately simple, yet it has been verified to handle tens of terabytes and millions of files in practice.
