# lesson-payload-v1

`lesson-payload-v1.bin` is the default owned payload for the local BitTorrent
learning swarm. It is exactly 5 MiB and begins with a short text header followed
by zero-filled bytes, making it deterministic and inexpensive to store.

With the default 256 KiB piece length, the torrent contains 20 pieces.
`lesson-payload-v1.torrent` uses the local Compose tracker URL
`http://tracker:6969/announce`.

Current local artifact identifiers:

```text
payload SHA-256:  556c4f1388fd6ad7b9f5e78ed5da7eda13e7b231f0e0be99d639b7ba1b4d0b45
info hash:        d8447158907ccaf3367f008520f19dd8997dfd51
metainfo SHA-256: 9fd19f8af038b3b06b67b948e5e5e5a1d6ec6025da0272f1cf8e3610acde9df5
```
