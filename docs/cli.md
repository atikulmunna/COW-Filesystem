# CLI Reference

## Mount and Unmount

```bash
cowfs mount <storage_dir> <mount_point> [--debug]
cowfs umount <mount_point>
```

## File History and Restore

```bash
cowfs history <file_path> --storage <storage_dir> [--json]
cowfs restore <file_path> (--version <n> | --before "<datetime>") --storage <storage_dir> [--dry-run] [--json]
```

## Activity Log

```bash
cowfs log --storage <storage_dir> [--limit <n>] [--action <name>] [--path-prefix <prefix>] [--since "<datetime>"] [--until "<datetime>"] [--json]
```

## Diff and Stats

```bash
cowfs diff <file_path> (--version <n> | --v1 <n> --v2 <m>) --storage <storage_dir> [--json]
cowfs stats --storage <storage_dir> [--json]
```

## Garbage Collection

```bash
cowfs gc --storage <storage_dir> [--dry-run] [--keep-last <n>] [--before "<datetime>"] [--json]
```

## Snapshots

```bash
cowfs snapshot create <name> [--description "..."] --storage <storage_dir> [--json]
cowfs snapshot list --storage <storage_dir> [--json]
cowfs snapshot show <name> --storage <storage_dir> [--json]
cowfs snapshot restore <name> [--keep-new] --storage <storage_dir> [--dry-run] [--json]
cowfs snapshot delete <name> --storage <storage_dir> [--json]
```
