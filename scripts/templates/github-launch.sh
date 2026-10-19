#!/bin/sh
set -eu
umask 077
channel='@CHANNEL@'
plugin_root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd -P)
state_root="$HOME/Library/Application Support/GhostSentinel/github/$channel"
mode=run
if [ "${1-}" = '--verify' ]; then mode=verify; shift; fi
if [ "${1-}" = '--state-root' ] && [ "$#" -eq 2 ]; then state_root=$2; shift 2; fi
[ "$#" -eq 0 ] || { echo 'invalid launcher arguments' >&2; exit 1; }
case "$state_root" in /*/"$channel") ;; *) echo 'invalid channel state root' >&2; exit 1;; esac
[ "$(uname -s)" = Darwin ] && [ "$(uname -m)" = arm64 ] || { echo 'GhostSentinel requires macOS Apple Silicon' >&2; exit 1; }
owner=$(id -u)
private_dir() {
  [ ! -L "$1" ] || { echo 'symlink state directory refused' >&2; exit 1; }
  [ -d "$1" ] || mkdir -m 700 "$1"
  [ "$(stat -f '%u:%Lp' "$1")" = "$owner:700" ] || { echo 'private state directory required' >&2; exit 1; }
}
# Parents owned by the installation are private; Library/Application Support
# itself is an ordinary user directory and is never chmod'ed by this launcher.
if [ "$state_root" = "$HOME/Library/Application Support/GhostSentinel/github/$channel" ]; then
  private_dir "$HOME/Library/Application Support/GhostSentinel"
  private_dir "$HOME/Library/Application Support/GhostSentinel/github"
else
  [ -d "$(dirname -- "$state_root")" ] && [ ! -L "$(dirname -- "$state_root")" ] || exit 1
fi
private_dir "$state_root"
private_dir "$state_root/payloads"
archive_sha=$(cat "$plugin_root/payload.sha256")
manifest_sha=$(cat "$plugin_root/payload.manifest.sha256")
case "$archive_sha:$manifest_sha" in *[!0-9a-f:]*|:*) exit 1;; esac
[ "${#archive_sha}" -eq 64 ] && [ "${#manifest_sha}" -eq 64 ] || exit 1
[ "$(shasum -a 256 "$plugin_root/payload.tar.gz" | cut -d ' ' -f 1)" = "$archive_sha" ] || { echo 'payload archive checksum mismatch' >&2; exit 1; }
release="$state_root/payloads/$archive_sha"
lock="$state_root/payloads/.unpack-$archive_sha.lock"
partial=''
locked=false
cleanup() {
  [ -z "$partial" ] || rm -rf -- "$partial"
  if [ "$locked" = true ]; then rmdir -- "$lock"; fi
}
trap cleanup EXIT
trap 'exit 1' HUP INT TERM
if [ ! -d "$release" ]; then
  attempt=0
  until mkdir -m 700 "$lock" 2>/dev/null; do
    attempt=$((attempt + 1)); [ "$attempt" -lt 30 ] || { echo 'payload preparation busy' >&2; exit 1; }
    sleep 1
  done
  locked=true
  if [ ! -d "$release" ]; then
    partial=$(mktemp -d "$state_root/payloads/.extract.XXXXXXXX")
    COPYFILE_DISABLE=1 tar -xzf "$plugin_root/payload.tar.gz" -C "$partial"
    [ ! -L "$partial/runtime/node" ] && [ "$(shasum -a 256 "$partial/runtime/node" | cut -d ' ' -f 1)" = '@NODE_SHA256@' ] || exit 1
    "$partial/runtime/node" --input-type=module - "$partial" "$release" "$manifest_sha" <<'VERIFY'
import { pathToFileURL } from 'node:url'
import { rename, lstat } from 'node:fs/promises'
const [root, target, digest] = process.argv.slice(2)
const { inspectPluginPackage } = await import(pathToFileURL(root + '/chatgpt/plugin-package.mjs'))
await inspectPluginPackage(root, { digest })
if (await lstat(target).catch(error => { if (error.code === 'ENOENT') return null; throw error })) throw new Error('payload target already exists')
await rename(root, target)
VERIFY
    partial=''
  fi
  rmdir -- "$lock"; locked=false
fi
private_dir "$release"
[ ! -L "$release/runtime/node" ] && [ "$(shasum -a 256 "$release/runtime/node" | cut -d ' ' -f 1)" = '@NODE_SHA256@' ] || exit 1
[ "$(shasum -a 256 "$release/plugin-package.json" | cut -d ' ' -f 1)" = "$manifest_sha" ] || exit 1
trap - EXIT HUP INT TERM
if [ "$mode" = verify ]; then
  exec "$release/runtime/node" --input-type=module - "$release" "$manifest_sha" "$channel" <<'VERIFY_ONLY'
import { pathToFileURL } from 'node:url'
const [root, digest, channel] = process.argv.slice(2)
const { inspectPluginPackage } = await import(pathToFileURL(root + '/chatgpt/plugin-package.mjs'))
const value = await inspectPluginPackage(root, { digest })
console.log(JSON.stringify({ status: 'verified', channel, version: value.manifest.version, manifestSHA256: value.digest, launched: false }))
VERIFY_ONLY
fi
exec "$release/runtime/node" "$release/chatgpt/github-plugin-bridge.mjs" --channel "$channel" --release "$release" --archive-sha "$archive_sha" --state-root "$state_root"
