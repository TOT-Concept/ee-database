#!/usr/bin/env bash
set -euo pipefail

# ──────────────────────────────────────────────────────────────────
#  ee-database — open-source sync client for Entity Enricher
#
#  Source:   https://github.com/TOT-Concept/ee-database
#  License:  MIT
#  Releases: https://github.com/TOT-Concept/ee-database/releases
#  Issues:   https://github.com/TOT-Concept/ee-database/issues
#
#  This installer is also open source. Audit it before running:
#    curl -fsSL https://entityenricher.ai/install-eedatabase.sh | less
# ──────────────────────────────────────────────────────────────────

REPO="TOT-Concept/ee-database"
VERSION="${EE_DATABASE_VERSION:-latest}"

OS=$(uname -s | tr '[:upper:]' '[:lower:]')           # darwin | linux
ARCH=$(uname -m)
case "$ARCH" in
  aarch64|arm64) ARCH=arm64 ;;
  x86_64|amd64)  ARCH=x86_64 ;;
  *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
esac
case "$OS" in
  darwin|linux) : ;;
  *) echo "Unsupported OS: $OS (use install-eedatabase.ps1 on Windows)" >&2; exit 1 ;;
esac

ASSET="ee-database-${OS}-${ARCH}"
if [ "$VERSION" = "latest" ]; then
  URL="https://github.com/${REPO}/releases/latest/download/${ASSET}"
  SIG_URL="https://github.com/${REPO}/releases/latest/download/${ASSET}.sig"
else
  URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}"
  SIG_URL="https://github.com/${REPO}/releases/download/${VERSION}/${ASSET}.sig"
fi

# cosign public key used to verify release-signed binaries.
COSIGN_PUBKEY_URL="${EE_DATABASE_COSIGN_PUBKEY_URL:-https://entityenricher.ai/ee-database-cosign.pub}"

if [ -w /usr/local/bin ]; then
  DEST="/usr/local/bin/ee-database"
elif [ -w "${HOME}/.local/bin" ] || mkdir -p "${HOME}/.local/bin" 2>/dev/null; then
  DEST="${HOME}/.local/bin/ee-database"
else
  echo "Could not find a writable install directory. Re-run with sudo, or" >&2
  echo "ensure ~/.local/bin exists and is writable." >&2
  exit 1
fi

cat <<EOF
ee-database installer

  Source     https://github.com/${REPO}  (MIT)
  Download   ${URL}
  Signature  cosign-verified against ${COSIGN_PUBKEY_URL}
  Install to ${DEST}
  Version    ${VERSION}

You can audit this script first:    curl -fsSL https://entityenricher.ai/install-eedatabase.sh | less
You can pin a version:               EE_DATABASE_VERSION=v1.0.0 curl ... | sh
You can skip the prompt in CI:       EE_DATABASE_YES=1 curl ... | sh
You can skip PATH setup:             EE_DATABASE_NO_PATH=1 (manage shell rc yourself)
You can skip signature verification: EE_DATABASE_SKIP_VERIFY=1 (not recommended)

EOF

# 5-second pause so a piped 'curl | sh' has a Ctrl+C window. Skip if
# stdin is non-interactive (CI) or the user opted out via EE_DATABASE_YES=1.
if [ -t 0 ] && [ -z "${EE_DATABASE_YES:-}" ]; then
  printf "Continuing in 5 seconds (Ctrl+C to abort)... "
  for i in 5 4 3 2 1; do
    printf "%d " "$i"
    sleep 1
  done
  printf "\n\n"
fi

mkdir -p "$(dirname "$DEST")"

TMP=$(mktemp)
TMP_SIG=$(mktemp)
TMP_PUB=$(mktemp)
trap 'rm -f "$TMP" "$TMP_SIG" "$TMP_PUB"' EXIT

if ! curl -fsSL "$URL" -o "$TMP"; then
  echo "Download failed from $URL" >&2
  echo "If you're installing from a non-public release, check the URL or pin a version." >&2
  exit 1
fi

# === Signature verification =================================================
# We download the binary's .sig + the project's cosign public key, then verify.
# Skipped only when the user opts out via EE_DATABASE_SKIP_VERIFY=1, in which
# case we print a loud warning so it's never silent.
if [ -z "${EE_DATABASE_SKIP_VERIFY:-}" ]; then
  if ! command -v cosign >/dev/null 2>&1; then
    echo "cosign is required to verify the binary signature." >&2
    echo "  Install:  https://docs.sigstore.dev/cosign/installation/" >&2
    echo "  macOS:    brew install cosign" >&2
    echo "  Linux:    see installation docs above" >&2
    echo "  Bypass:   EE_DATABASE_SKIP_VERIFY=1 (not recommended)" >&2
    exit 1
  fi
  if ! curl -fsSL "$SIG_URL" -o "$TMP_SIG"; then
    echo "Could not download signature from $SIG_URL" >&2
    echo "  Bypass:   EE_DATABASE_SKIP_VERIFY=1 (not recommended)" >&2
    exit 1
  fi
  if ! curl -fsSL "$COSIGN_PUBKEY_URL" -o "$TMP_PUB"; then
    echo "Could not download cosign public key from $COSIGN_PUBKEY_URL" >&2
    exit 1
  fi
  if ! cosign verify-blob --key "$TMP_PUB" --signature "$TMP_SIG" "$TMP" >/dev/null 2>&1; then
    echo "✗ Signature verification FAILED for $ASSET" >&2
    echo "  The downloaded binary does not match the published signature." >&2
    echo "  This could mean tampering or a partial download. Aborting." >&2
    exit 1
  fi
  echo "✓ Signature verified."
else
  echo "⚠  EE_DATABASE_SKIP_VERIFY=1 set — proceeding without signature check." >&2
fi

chmod +x "$TMP"
mv "$TMP" "$DEST"
trap - EXIT
rm -f "$TMP_SIG" "$TMP_PUB"

echo "✓ Installed ee-database to $DEST"

# === PATH setup ============================================================
# If we landed in ~/.local/bin and it isn't on PATH, append an export line to
# the user's shell rc file. Idempotent via a marker comment. Opt out with
# EE_DATABASE_NO_PATH=1. /usr/local/bin is assumed to be on PATH already.
INSTALL_DIR=$(dirname "$DEST")
NEEDS_SHELL_RELOAD=0
if [ -z "${EE_DATABASE_NO_PATH:-}" ] && [ "$INSTALL_DIR" = "${HOME}/.local/bin" ]; then
  case ":${PATH}:" in
    *":${INSTALL_DIR}:"*)
      : # already on PATH, nothing to do
      ;;
    *)
      SHELL_NAME=$(basename "${SHELL:-/bin/sh}")
      case "$SHELL_NAME" in
        zsh)
          RC_FILE="${ZDOTDIR:-$HOME}/.zshrc"
          EXPORT_LINE='export PATH="$HOME/.local/bin:$PATH"'
          ;;
        bash)
          # macOS Terminal launches bash as a login shell, which reads
          # .bash_profile (not .bashrc). Linux defaults to interactive
          # non-login, which reads .bashrc.
          if [ "$OS" = "darwin" ]; then
            RC_FILE="$HOME/.bash_profile"
          else
            RC_FILE="$HOME/.bashrc"
          fi
          EXPORT_LINE='export PATH="$HOME/.local/bin:$PATH"'
          ;;
        fish)
          RC_FILE="$HOME/.config/fish/config.fish"
          EXPORT_LINE='set -gx PATH "$HOME/.local/bin" $PATH'
          ;;
        *)
          RC_FILE="$HOME/.profile"
          EXPORT_LINE='export PATH="$HOME/.local/bin:$PATH"'
          ;;
      esac

      MARKER="# Added by ee-database installer"
      if [ -f "$RC_FILE" ] && grep -Fq "$MARKER" "$RC_FILE" 2>/dev/null; then
        echo "ℹ  PATH entry already present in $RC_FILE (skipping)."
      else
        mkdir -p "$(dirname "$RC_FILE")"
        {
          printf '\n%s\n%s\n' "$MARKER" "$EXPORT_LINE"
        } >> "$RC_FILE"
        echo "✓ Added $INSTALL_DIR to PATH in $RC_FILE"
        NEEDS_SHELL_RELOAD=1
      fi
      ;;
  esac
fi

echo
echo "Next steps:"
echo "  1. Register a database in the Entity Enricher UI."
echo "     (Databases page → Sync client → Pair a client)"
echo "  2. Copy the pairing command shown there and run it here."
echo "  3. Run:  ee-database run --dsn <target-database-dsn> --save-dsn --create-missing"
if [ "$NEEDS_SHELL_RELOAD" = "1" ]; then
  echo
  echo "⚠  Open a new terminal (or 'source $RC_FILE') so the updated PATH"
  echo "   takes effect in this shell."
fi
