<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**

- [Working Features](#working-features)
- [Known issues](#known-issues)
  - [S3 Restore Performance / Considerations](#s3-restore-performance--considerations)
  - [Sizes shown in PVE frontend](#sizes-shown-in-pve-frontend)
- [Usage](#usage)
- [Quickstart](#quickstart)
    - [Setup minio](#setup-minio)
    - [PVE configuration: direct backup](#pve-configuration-direct-backup)
    - [PBS configuration: using the proxy as remote](#pbs-configuration-using-the-proxy-as-remote)
    - [Proxmox backup client](#proxmox-backup-client)
- [Running with Docker](#running-with-docker)
- [Notes](#notes)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

WIP!! 
Use as follows

Note: Garbage collector is experimental, use with extreme caution

# Working Features

The following features are currently implemented:

 * As with PBS 3.3+ you can use the PBS [push and pull features](https://pbs.proxmox.com/docs/managing-remotes.html) for syncing your
   backups to/from S3 compatible backends.
 * Configure proxy in PVE and use it for both CT and VM backups (full and incremental)
 * Restore functionality (VM restore, mount, map)
 * Basic PVE UI integration: adding notes, setting the protection flag,
   removing backups, showing configuration.
 * File backup/restore/mount via proxmox-backup-client (full and incremental)

# Known issues
## S3 Restore Performance / Considerations

Both the proxmox VM and file backup client will split the backups into many
small chunks, S3 is not known to perform well upon reading many small files.

If using the proxy as direct datastore, the more likely you will see
performance issues during restore, depending on the amount of objects stored.

Using the PBS Push or Pull functions can greatly enhance speeds to sync your
backups, as they are done in parallel.

Also think about cost peaks for object read operations. The proxmox backup
clients will request required chunks sequentially, there is currently no
way to optimize this in the proxy.

You should consider twice if you want to make the S3 backend your primary
backup storage. For local S3 instances with S3 compatible API (ceph, minio) the
performance depends largely on your setup.

As with proxmox 8.2, a feature called "Backup fleecing" was introduced, [See
release notes](https://pve.proxmox.com/wiki/Roadmap#Proxmox_VE_8.2) which
prevents VM lockup / slow down in case of slow backup storage, which is more
likely to happen with hosted S3 over slow network connections.

## Sizes shown in PVE frontend

The size column of the backup list shows, by default, the logical size of the
snapshot: the size of the guest disks it contains, which is what an index
states in its header and what Proxmox Backup Server itself reports. It is not
the space the snapshot occupies in the bucket, since chunks are shared between
snapshots.

`-snapshotsize` selects another figure:

| Mode | Reports | Answers |
|---|---|---|
| `logical` (default) | size of the guest disks | what a restore produces |
| `referenced` | size of the chunks it points at, shared ones included | what this backup would cost on its own |
| `exclusive` | size of the chunks no other backup points at | what deleting it would actually free |

`referenced` and `exclusive` come from the report written by the garbage
collector, so they are at most one collector run old, and a snapshot taken
since the last run falls back to its logical size rather than showing zero.

The usage gauge of the storage shows the real size of the bucket. Free space
cannot be derived from S3: a bucket has no capacity to read back. Pass
`-datastoresize` to declare one, otherwise a large free space is reported
rather than showing a store with no quota as full.

Deduplication figures - what a snapshot references, and what deleting it would
actually free - are computed by the garbage collector and stored in the bucket
as `usage-stats.json`. They cannot be produced on demand, since knowing what a
snapshot holds exclusively means resolving the chunk references of every other
snapshot. Read them back with:

```
GET /api2/json/admin/datastore/<datastore>/s3stats
```

## Deleting a backup does not free the space immediately

Deleting a snapshot, from the PVE interface or otherwise, removes the objects
of that snapshot only: its indexes, its blobs and its log. The chunks holding
the data are shared with other snapshots and are never removed at that point.

They are reclaimed by the garbage collector, which keeps every chunk that any
remaining index still references. Space therefore comes back at the next
collector run, not at deletion time.

# Usage

```
Usage of ./pmoxs3backuproxy:
  -bind string
        PBS Protocol bind address, recommended 127.0.0.1:8007, use :8007 for all (default "127.0.0.1:8007")
  -bucketcachettl uint
        Seconds a datastore (bucket) listing is reused before asking the S3 endpoint again, 0 disables caching (default 60)
  -cert string
        Server SSL certificate file (default "server.crt")
  -chunks3timeout uint
        Maximum seconds for one chunk S3 operation, 0 disables the deadline (default 300)
  -datastoresize uint
        Capacity of the datastore in bytes, used to report free space, 0 if the bucket has no quota
  -debug
        Debug logging
  -endpoint string
        S3 Endpoint without https/http , host:port
  -key string
        Server SSL key file (default "server.key")
  -lookuptype string
        Bucket lookup type: auto,dns,path (default: "auto")
  -snapshotsize string
        Size reported for a backup: logical (guest disk size), referenced (chunks it points at) or exclusive (chunks only it points at) (default "logical")
  -snapshotcachettl uint
        Seconds a snapshot listing is reused before listing the bucket again, 0 disables caching (default 30)
  -usagecachettl uint
        Seconds before the used space of a datastore is recomputed in the background (default 900)
  -usessl
        Enable SSL connection to the endpoint, for use with cloud S3 providers
```

Proxmox VE polls this API constantly - pvestatd refreshes every storage every
10 seconds - and gives it 7 seconds to answer, a timeout hardcoded in
`PVE::Storage::PBSPlugin`. Listings are therefore cached, and expensive figures
such as the used space are refreshed in the background rather than on the
request path. Without that, a transient slowdown of the object store makes
`vzdump` fail its pre-flight check with `error fetching datastores - 500 read
timeout` and abort the whole backup job.

Concurrent requests for the same content-addressed chunk are coalesced into a
single S3 operation. Duplicate HTTP/2 request bodies are still drained while
that operation is in progress, so they cannot consume the connection flow
control window and block the upload. `-chunks3timeout` bounds the complete S3
operation for one chunk; keep the default unless a 4 MiB chunk can legitimately
take more than five minutes to upload.

```
Usage of ./garbagecollector:
  -accesskey string
        S3 Access Key ID
  -bucket string
        Bucket to perform garbage collection on
  -chunkgrace uint
        Hours a chunk is protected from orphan removal after being written, 0 disables the protection (default 24)
  -debug
        Debug logging
  -endpoint string
        S3 Endpoint without https/http , host:port
  -lookuptype string
        Bucket lookup type: auto,dns,path (default: "auto")
  -retention uint
        Number of days to keep backups for (default 60)
  -secretkey string
        S3 Secret Key, discouraged , use a file if possible
  -secretkeyfile string
        S3 Secret Key File
  -usessl
        Use SSL for endpoint connection: default: false

```

A chunk is uploaded before the index referencing it is written, so the chunks
of a backup that is still running are indistinguishable from orphans. The
collector therefore leaves recently written chunks alone and collects them on a
later run; `-chunkgrace` sets how recent is recent enough. Do not disable it
unless no backup can possibly run at the same time.

Each run also writes `usage-stats.json` at the root of the bucket, holding for
every snapshot its logical size, the size of the chunks it references and the
size of the chunks no other snapshot references.

# Quickstart
### Setup minio

Start minio server (either on the PVE system or on a remote system),
specify the listening IP via `--address`

```
minio server ~/minio --address 127.0.0.1:9000
mc alias set 'myminio' 'http://127.0.0.1:9000' 'minioadmin' 'minioadmin'
```

Create the target bucket used as datastore:

```
mc mb myminio/backups
```

Create an API access key for the minio admin account:

```
mc admin user svcacct add myminio minioadmin
Access Key: 431EM4CTA0OP810W6FER
Secret Key: RIa82lyl6ZrEYVtvwaMgh2JFlOISENiGQT+Lv0IE
Expiration: no-expiry
```

Start the proxy via:

```
pmoxs3backuproxy -endpoint 127.0.0.1:9000
```

### PVE configuration: direct backup

Add the proxy endpoint as proxmox backup server storage using the following address:

`127.0.0.1:8007`

Use

```
55:BC:29:4B:BA:B6:A1:03:42:A9:D8:51:14:9D:BD:00:D2:2A:9C:A1:B8:4A:85:E1:AF:B2:0C:48:40:D6:CC:A4
```

`Note:` if you intend to run the proxy on a public network, please replace the
included certificates with your own to prevent possible MITM attacks.

Use the created access_key@pbs for username and secret key as password, bucket
as datastore


### PBS configuration: using the proxy as remote

If you want to use the proxy as remote datastore in PBS, configure it as
described in the [proxmox manual](https://pbs.proxmox.com/docs/managing-remotes.html)


### Proxmox backup client

Use proxmox backup client by setting the repository and password accordingly:

```
 export PBS_PASSWORD=<secret>
 proxmox-backup-client backup root.pxar:/etc --repository <access_key>@pbs@127.0.0.1:<bucket>
```

# Running with Docker

Add the following to your `docker-compose.yml`, add/update your `-endpoint`, then run `docker compose up -d`. The service will be accessible at `localhost:8007`.
```
name: pmoxs3backuproxy
services:
  pmoxs3backuproxy:
    image: ghcr.io/tizbac/pmoxs3backuproxy:latest
    command: -bind 127.0.0.1:8007 -endpoint 127.0.0.1:9000
    container_name: pmoxs3backuproxy
    hostname: pmoxs3backuproxy
    restart: unless-stopped
    volumes:
      - /etc/localtime:/etc/localtime:ro
    ports:
      - '8007:8007'
```

For increased security, you can add the following security parameters without affecting container function:
```
    user: '65532:65532'
    security_opt:
      - no-new-privileges:true
    cap_drop:
      - ALL
```
To execute the garbage collector in a separate container, you can use a
different entrypoint:
```
 docker run --entrypoint /garbagecollector -it ghcr.io/tizbac/pmoxs3backuproxy:latest [..]
```
or exec it within the running proxy container:
```
 docker exec pmoxs3backuproxy /garbagecollector [..]
```

# Notes

Garbage collector process ( scheduled with crontab ) must absolutely run on
same machine as the proxy for locking to work!

Garbage collector will also check for integrity ( only the presence of all
referenced chunks ), if a backup is found to be broken, it will not be deleted
and retention will be honored, but it will be marked corrupted, so next backup
from PVE will be non incremental and will recreate missing chunk if needed.
Corrupted backup will not appear in PVE backup list
