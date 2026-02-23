#!/bin/bash
# End-to-end test for COWFS
set -o pipefail
VENV="/home/munna/projects/cowfs/.venv/bin"
STORAGE="/tmp/cowfs_e2e_storage"
MOUNT="/tmp/cowfs_e2e_mnt"
PASS=0
FAIL=0

cleanup() {
    fusermount3 -u "$MOUNT" 2>/dev/null
    sleep 0.5
    rm -rf "$STORAGE" "$MOUNT"
}

assert_eq() {
    local desc="$1" expected="$2" actual="$3"
    if [ "$expected" = "$actual" ]; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc (expected='$expected', got='$actual')"
        FAIL=$((FAIL + 1))
    fi
}

assert_ok() {
    local desc="$1"
    shift
    if "$@" 2>/dev/null; then
        echo "PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "FAIL: $desc (exit=$?)"
        FAIL=$((FAIL + 1))
    fi
}

# Cleanup any previous run
cleanup 2>/dev/null
mkdir -p "$STORAGE" "$MOUNT"

# Mount
$VENV/cowfs mount "$STORAGE" "$MOUNT" &>/dev/null &
COWFS_PID=$!
sleep 2

# Verify mounted
if mountpoint -q "$MOUNT"; then
    echo "PASS: filesystem mounted"
    PASS=$((PASS + 1))
else
    echo "FAIL: filesystem not mounted"
    cleanup
    exit 1
fi

# Test 1: Create and read a file
echo "hello cowfs" > "$MOUNT/test.txt"
assert_eq "write/read file" "hello cowfs" "$(cat "$MOUNT/test.txt")"

# Test 2: Create a directory
assert_ok "mkdir" mkdir "$MOUNT/subdir"

# Test 3: Write in subdirectory
echo "nested content" > "$MOUNT/subdir/inner.txt"
assert_eq "nested write/read" "nested content" "$(cat "$MOUNT/subdir/inner.txt")"

# Test 4: List root directory
LS_ROOT=$(ls "$MOUNT/" | sort | tr '\n' ' ' | sed 's/ $//')
assert_eq "ls root" "subdir test.txt" "$LS_ROOT"

# Test 5: List subdirectory
LS_SUB=$(ls "$MOUNT/subdir/")
assert_eq "ls subdir" "inner.txt" "$LS_SUB"

# Test 6: Overwrite file
echo "new content" > "$MOUNT/test.txt"
assert_eq "overwrite" "new content" "$(cat "$MOUNT/test.txt")"

# Test 7: Delete file
assert_ok "delete file" rm "$MOUNT/test.txt"

# Test 8: File gone after delete
LS_AFTER=$(ls "$MOUNT/" | sort | tr '\n' ' ' | sed 's/ $//')
assert_eq "ls after delete" "subdir" "$LS_AFTER"

# Test 9: rmdir
assert_ok "delete nested file" rm "$MOUNT/subdir/inner.txt"
assert_ok "rmdir" rmdir "$MOUNT/subdir"

# Test 10: Empty root after cleanup
LS_EMPTY=$(ls "$MOUNT/" 2>/dev/null)
assert_eq "empty root" "" "$LS_EMPTY"

# Check process is still alive
if kill -0 $COWFS_PID 2>/dev/null; then
    echo "PASS: cowfs process survived all operations"
    PASS=$((PASS + 1))
else
    echo "FAIL: cowfs process crashed"
    FAIL=$((FAIL + 1))
fi

# Unmount
fusermount3 -u "$MOUNT"
wait $COWFS_PID 2>/dev/null || true
echo "PASS: unmounted cleanly"
PASS=$((PASS + 1))

# Summary
echo ""
echo "=== Results: $PASS passed, $FAIL failed ==="
cleanup 2>/dev/null
[ $FAIL -eq 0 ] && exit 0 || exit 1
