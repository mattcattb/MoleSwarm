# tiny-file-v1

`tiny-file-v1.txt` is a small, owned payload for the first BitTorrent learning
swarm.

`tiny-file-v1.torrent` is generated from this exact file with the local Docker
Compose tracker URL `http://tracker:6969/announce`. It is valid for the local
container demonstration. When the Railway tracker has a stable public URL,
regenerate the metainfo from this unchanged payload using that URL before
publishing the deployment artifact.

Current local artifact identifiers:

```text
info hash:        858a579922bf4b78cd4c86a7da88b5d6e85638ba
metainfo SHA-256: db04ab5cbdec1fa3a4f330b2107bafaaf540bece530e7c93703a19321e0beb0e
```
