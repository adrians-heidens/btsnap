#!/usr/bin/env python3

"""Collection of scripts for specific workflow of btrfs backups.

snapshot-volume -- a volume which contains snapshots in form of {date}@{tag}.
For example, a volume /var/storage/.snapshots/movies containing snapshots
2026-03-07T21:52:32.099615@daily

snapshot-super-volume -- a volume with many snapshot-volumes..
For example, a volume /var/storage/.snapshots containing subvolumes
music, pictures, documents, etc. where each of them contains
dated+tagged snapshots.
"""

import os
import re
import pathlib
import subprocess
import datetime
import argparse


def create_snapshot(snapdir, source, tag, now):
    """Create btrfs snapshot of volume 'source' and store it in 'snapdir'.

    Example: snapdir=/foo/bar, source=/spam/eggs, tag=foo, now=2026-03-07:

    1) creates subvolume /foo/bar it not exist,
    2) creates subvol. /foo/bar/eggs if not exist,
    3) creates snapshot of /spam/eggs in /foo/bar/eggs/2026-03-07@foo
    """

    # Create snapdir.
    if not snapdir.exists():
        subprocess.run([
            "btrfs",
            "subvolume",
            "create",
            str(snapdir)
            ], check=True)

    # Create destination subvol.
    destvol = snapdir / source.name
    if not destvol.exists():
        subprocess.run([
            "btrfs",
            "subvolume",
            "create",
            str(destvol)
            ], check=True)

    # Create readonly snapshot named as datetime.
    dt_str = now.isoformat()
    dst = destvol / dt_str
    subprocess.run([
        "btrfs",
        "subvolume",
        "snapshot",
        "-r",
        str(source),
        str(dst) + f"@{tag}",
        ], check=True)


def send_snapshot(destvol, source, tag):
    """Send all snapshots of 'source' and 'tag' to another device location
    'destvol'. This checks already sent snapshots and do only new ones.

    Example: destvol=/foo/bar, source=/spam/.snapshots/eggs, tag=daily

    1) filters all snapshots of specified tag (@daily),
    2) checks destvol for tagged entries and find which is synced already
    3) go through unsynced snapshots, call btrfs send + btrfs receive
    """

    print(f"send snapshots of source={source}, tag={tag} to {destvol}")

    # Do nothing ir no source.
    if not source.exists():
        return

    # Create destination vol if not exist.
    if not destvol.exists():
        subprocess.run([
            "btrfs",
            "subvolume",
            "create",
            str(destvol)
            ], check=True)

    # Get already synced entries and current entries.
    synced_entries = []
    for name in os.listdir(destvol):
        if "@" in name:
            _, t = name.split("@")
            if t != tag:
                continue
        synced_entries.append(name)
    synced_entries.sort()

    entries = []
    for name in os.listdir(source):
        if "@" in name:
            _, t = name.split("@")
            if t != tag:
                continue
        entries.append(name)
    entries.sort()

    # Find latest sync point.
    last_sync = -1
    if len(synced_entries) > 0:
        try:
            last_sync = entries.index(synced_entries[-1])
        except ValueError:
            pass

    # Get through unsynced entries.
    index = 0
    if last_sync != -1:
        print(f"Last sync point: {synced_entries[-1]}")
        index = last_sync + 1
    while index < len(entries):
        # Get entry name.
        entry = entries[index]
        index += 1

        print("Sending snapshot: " + entry)

        # Compose send arguments.
        a = ["btrfs", "send"]
        if last_sync != -1:  # Add -p if previous snapshot exists.
            a.extend(["-p", str(source / entries[last_sync])])
        a.append(str(source / entry)) # Destination subvol.

        # Pipe send to receive through dd to collect stats.
        p1 = subprocess.Popen(a, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p2 = subprocess.Popen(["dd"], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p3 = subprocess.Popen([
            "btrfs",
            "receive",
            str(destvol),  # receive destination subvol
            ], stdin=p2.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        p3.wait()

        # Get and print data from dd
        data = str(p2.stderr.read(), "utf8")
        lines = data.splitlines()
        assert(len(lines) == 3)
        print(lines[2])

        # Update last_sync index.
        last_sync = index - 1


def tag_snapshots(subvol, tag):
    """Find latest snapshot and duplicate it with another tag."""

    entries = sorted(os.listdir(subvol))
    latest = subvol / entries[-1]

    name = latest.name
    if "@" in name:
        name = name.rsplit("@", 1)[0]
    name = name + f"@{tag}"

    dst = subvol / name

    if latest == dst:
        print("skip same")
        return

    subprocess.run([
        "btrfs",
        "subvolume",
        "snapshot",
        "-r",
        str(latest),
        str(dst)
        ], check=True)


def trim_snapshots(subvol, tag, size):
    """Delete the oldest snapshots with specified tag. Keep latest items only."""

    o = subprocess.run([
        "btrfs",
        "subvolume",
        "list",
        "-o",
        str(subvol)
        ], check=True, capture_output=True)

    items = []
    for line in o.stdout.splitlines():
        cols = line.split()
        path = pathlib.Path(str(cols[8], "utf8"))

        name_full = path.name
        if "@" in name_full:
            name, t = name_full.split("@")
            if t != tag:
                continue
        else:
            name = name_full

        if not re.match(r"^\d{4}-\d{2}-\d{2}T", name):
            raise Exception("Not snapshot dir")

        items.append(name_full)
    items.sort()

    index = 0
    while index < len(items) - size:
        path = subvol / items[index]
        print("delete", path)
        index += 1

        subprocess.run([
            "btrfs",
            "subvolume",
            "delete",
            str(path)
            ], check=True)


def cmd_create_snapshot():
    parser = argparse.ArgumentParser(
        description="Create snapshot of specified volume.")
    parser.add_argument("snapdir", type=pathlib.Path,
                        help="snapshot volume")
    parser.add_argument("source", type=pathlib.Path, nargs="+",
                        help="volume to create snapshot of")
    parser.add_argument("--tag", default="one",
                        help="tag of snapshot (default: %(default)s)")

    args = parser.parse_args()

    tag = args.tag
    if tag == "":
        tag = "one"
    now = datetime.datetime.now(datetime.UTC)
    for source in args.source:
        create_snapshot(args.snapdir, source, tag, now)


def cmd_create_all_snapshots():
    parser = argparse.ArgumentParser(
        description="Create snapshots of all subvolumes.")
    parser.add_argument("snapdir", type=pathlib.Path,
                        help="snapshot volume")
    parser.add_argument("source", type=pathlib.Path,
                        help="volume which subvolumes will be take snapshot of")
    parser.add_argument("--tag", default="one",
                        help="tag of snapshot (default: %(default)s)")

    args = parser.parse_args()

    # Get list of volumes to make snap of.
    o = subprocess.run([
        "btrfs",
        "subvolume",
        "list",
        "-o",
        str(args.source)
        ], capture_output=True, check=True)

    # Go though snap volumes and take snapshot.
    now = datetime.datetime.now(datetime.UTC)
    for line in o.stdout.splitlines():
        cols = line.split()
        path = pathlib.Path(str(cols[8], "utf8"))
        name = path.name
        if name.startswith("."):
            continue

        source = args.source / name
        create_snapshot(args.snapdir, source, args.tag, now)


def cmd_send_snapshots():
    parser = argparse.ArgumentParser(
        description="Send snapshots to another device volume.")
    parser.add_argument("destvol", type=pathlib.Path,
                        help="destination volume")
    parser.add_argument("source", type=pathlib.Path,
                        help="volume which contains snapshots to send")
    parser.add_argument("--tag", default="one",
                        help="tag of snapshot (default: %(default)s)")

    args = parser.parse_args()
    send_snapshot(args.destvol, args.source, args.tag)


def cmd_send_all_snapshots():
    parser = argparse.ArgumentParser(
        description="Send all snapshot directory to another device volume.")
    parser.add_argument("destvol", type=pathlib.Path,
                        help="destination volume")
    parser.add_argument("sourcedir", type=pathlib.Path,
                        help="snapshot directory to send")
    parser.add_argument("--tag", default="one",
                        help="tag of snapshot (default: %(default)s)")
    parser.add_argument("--trim-src", type=int,
                        help="trim src dir to this many snapshots")
    parser.add_argument("--trim-dst", type=int,
                        help="trim dst dir to this many snapshots")
    parser.add_argument("--create-destvol", action="store_true",
                        help="create destination volume if not exists")

    args = parser.parse_args()

    # Create destination volume.
    destvol = args.destvol
    if not destvol.exists() and args.create_destvol:
        subprocess.run([
            "btrfs",
            "subvolume",
            "create",
            str(destvol)
        ], check=True)
    elif not destvol.exists():
        raise Exception("Destination volume does not exist")

    # Get list of source volumes.
    o = subprocess.run([
        "btrfs",
        "subvolume",
        "list",
        "-o",
        str(args.sourcedir)
    ], capture_output=True, check=True)

    # Send each source vol to according destination and delete old snapshots.
    for line in o.stdout.splitlines():
        cols = line.split()
        path = pathlib.Path(str(cols[8], "utf8"))
        name = path.name
        if name.startswith("."):
            continue

        dest = destvol / name
        send_snapshot(dest, args.sourcedir / name, args.tag)

        if args.trim_dst is not None:
            trim_snapshots(dest, args.tag, args.trim_dst)

        if args.trim_src is not None:
            trim_snapshots(args.sourcedir / name, args.tag, args.trim_src)


def cmd_tag():
    parser = argparse.ArgumentParser(
        description="Duplicate latest snapshot with new tag.")
    parser.add_argument("subvol", type=pathlib.Path,
                        help="target snapshot volume")
    parser.add_argument("--tag", default="one",
                        help="tag to create (default: %(default)s)")

    args = parser.parse_args()
    tag_snapshots(args.subvol, args.tag)


def cmd_tag_all():
    parser = argparse.ArgumentParser(
        description="Duplicate latest snapshot for each subvolume.")
    parser.add_argument("subvol", type=pathlib.Path,
                        help="snapshot super volume to tag")
    parser.add_argument("--tag", default="one",
                        help="tag to create (default: %(default)s)")

    args = parser.parse_args()

    subvol = args.subvol
    o = subprocess.run([
        "btrfs",
        "subvolume",
        "list",
        "-o",
        str(subvol)
    ], check=True, capture_output=True)

    for line in o.stdout.splitlines():
        cols = line.split()
        path = pathlib.Path(str(cols[8], "utf8"))
        if path.name.startswith("."):
            continue

        vol = subvol.parent / path
        tag_snapshots(vol, args.tag)


def cmd_trim():
    parser = argparse.ArgumentParser(
        description="delete snapshots, keep only specified size of newest."
    )
    parser.add_argument("subvol", type=pathlib.Path,
                        help="target snapshot volume")
    parser.add_argument("tag",
                        help="trim snapshots of this tag")
    parser.add_argument("size", type=int,
                        help="number of snapshots to leave")

    args = parser.parse_args()
    subvol = args.subvol
    if not subvol.exists():
        return

    trim_snapshots(subvol, args.tag, args.size)


def cmd_trim_all():
    parser = argparse.ArgumentParser()
    parser.add_argument("subvol", type=pathlib.Path,
                        help="target snapshot super volume")
    parser.add_argument("tag",
                        help="trim snapshots of this tag")
    parser.add_argument("size", type=int,
                        help="number of snapshots to leave")

    args = parser.parse_args()

    subvol = args.subvol
    if not subvol.exists():
        return

    o = subprocess.run([
        "btrfs",
        "subvolume",
        "list",
        "-o",
        str(subvol)
        ], check=True, capture_output=True)

    for line in o.stdout.splitlines():
        cols = line.split()
        path = pathlib.Path(str(cols[8], "utf8"))
        if path.name.startswith("."):
            continue

        vol = subvol.parent / path
        print("vol:", vol)
        trim_snapshots(vol, args.tag, args.size)
