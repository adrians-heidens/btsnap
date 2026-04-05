#!/usr/bin/env python3

"""Collection of scripts for specific workflow of btrfs backups.

snapshot-volume -- a volume which contains snapshots in form of {date}@{tag}.
For example, a volume /var/storage/.snapshots/movies containing snapshots
2026-03-07T21:52:32.099615@daily

snapshot-super-volume -- a volume with many snapshot-volumes..
For example, a volume /var/storage/.snapshots containing subvolumes
music, pictures, documents, etc. where each of them contains
dated+tagged snapshots.

An example setup:

/var/storage/ -- volume containing specific subvolumes
/var/storage/documents/ -- subvolume of documents
/var/storage/music/ -- subvolume of music
/var/storage/pictures/ -- subvolume of pictures

/var/storage/.snapshots/ -- snapshot super volume containing all snapshots
/var/storage/.snapshots/documents/ -- snapshot volume of documents
    2026-03-07T21:52:32.099615@daily -- dated, tagged snapshot
    2026-03-08T21:52:32.099615@daily
/var/storage/.snapshots/music/ -- snapshot volume of music
/var/storage/.snapshots/pictures/ -- snapshot volume of pictures

/var/backup/ -- another device snapshot super volume for sending backups
/var/backup/documents/ -- backup of documents snapshot volume
    2026-03-07T21:52:32.099615@daily
    2026-03-08T21:52:32.099615@daily
/var/backup/music/
/var/backup/pictures/
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


def send_volume(srcvol, destvol):
    # Check srcvol.
    subprocess.run([
        "btrfs",
        "subvolume",
        "show",
        str(srcvol)
    ], check=True, capture_output=True)

    # Create destvol if needed.
    if not destvol.exists():
        subprocess.run([
            "btrfs",
            "subvolume",
            "create",
            str(destvol)
        ], check=True)

    # Do send-receive.
    p1 = subprocess.Popen(["btrfs", "send", srcvol], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    p2 = subprocess.Popen(["btrfs", "receive", destvol], stdin=p1.stdout, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    o = p2.wait()

    if o != 0:
        m = p2.stdout.read().decode("utf-8")
        raise Exception("Failed to send volume", m)


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


def cmd_create(args):
    now = datetime.datetime.now(datetime.UTC)
    for path in args.srcvol:
        path_list = []
        if "*" in path.name:
            path_list.extend(path.parent.glob(str(path.name)))
        else:
            path_list.append(path)

        for path in path_list:
            if path.name.startswith("."):
                continue
            create_snapshot(args.snapvol, path, args.tag, now)
    exit(0)


def cmd_send(args):
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

    # Send each source vol to according destination and delete old snapshots.
    for path in args.snapvol:
        path_list = []
        if "*" in path.name:
            path_list.extend(path.parent.glob(str(path.name)))
        else:
            path_list.append(path)

        for path in path_list:
            if path.name.startswith("."):
                continue

            dest = destvol / path.name
            send_snapshot(dest, path, args.tag)

            if args.trim_dst is not None:
                trim_snapshots(dest, args.tag, args.trim_dst)
            if args.trim_src is not None:
                trim_snapshots(path, args.tag, args.trim_src)

    exit(0)


def cmd_send2(args):
    srcvol = args.srcvol
    destvol = args.destvol

    send_volume(srcvol, destvol)

    exit(0)


def cmd_restore(args):
    # Get snapshots in snapvol.
    o = subprocess.run([
        "btrfs",
        "subvolume",
        "list",
        "-o",
        str(args.snapvol)
    ], check=True, capture_output=True)

    items = []
    for line in o.stdout.splitlines():
        cols = line.split()
        path = pathlib.Path(str(cols[8], "utf8"))
        items.append(path.name)
    if len(items) == 0:
        raise Exception("No snapshots found")

    items.sort()
    latest = items[-1]
    latest_full = args.snapvol / latest
    print("latest:", latest_full)

    subprocess.run([
        "btrfs",
        "subvolume",
        "snapshot",
        latest_full,
        args.destvol,
    ], check=True)

    exit(0)


def main():
    main_parser = argparse.ArgumentParser()
    subparsers = main_parser.add_subparsers()

    parser = subparsers.add_parser("create-snapshot",
        description="Create snapshots of volumes.")
    parser.set_defaults(func=cmd_create)
    parser.add_argument("snapvol", type=pathlib.Path,
                        help="snapshot volume")
    parser.add_argument("srcvol", type=pathlib.Path, nargs="+",
                        help="volume which subvolumes will be take snapshot of")
    parser.add_argument("--tag", default="daily",
                        help="tag of snapshot (default: %(default)s)")

    parser = subparsers.add_parser("send-snapshot",
        description="Send snapshot directory to another device volume.")
    parser.set_defaults(func=cmd_send)
    parser.add_argument("destvol", type=pathlib.Path,
                        help="destination volume")
    parser.add_argument("snapvol", type=pathlib.Path, nargs="+",
                        help="snapshot directory to send")
    parser.add_argument("--tag", default="daily",
                        help="tag of snapshot (default: %(default)s)")
    parser.add_argument("--trim-src", type=int,
                        help="trim src dir to this many snapshots")
    parser.add_argument("--trim-dst", type=int,
                        help="trim dst dir to this many snapshots")
    parser.add_argument("--create-destvol", action="store_true",
                        help="create destination volume if not exists")

    parser = subparsers.add_parser("send-volume",
        description="Send volume from one device to another.")
    parser.set_defaults(func=cmd_send2)
    parser.add_argument("srcvol", type=pathlib.Path,
                        help="volume to send")
    parser.add_argument("destvol", type=pathlib.Path,
                        help="volume to receive")

    parser = subparsers.add_parser("restore-snapshot",)
    parser.set_defaults(func=cmd_restore)
    parser.add_argument("snapvol", type=pathlib.Path)
    parser.add_argument("destvol", type=pathlib.Path)

    args = main_parser.parse_args()
    if "func" in args:
        args.func(args)
        exit(1)

    main_parser.print_help()
    exit(2)


if __name__ == '__main__':
    main()
