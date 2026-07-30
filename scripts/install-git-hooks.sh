#!/usr/bin/env bash
# Point this clone's git hooks at the tracked githooks/ directory.
#
# Installs three complementary guards (see each hook for its scope):
#   pre-commit  - forbidden identifiers in staged file CONTENT
#   commit-msg  - forbidden identifiers in the COMMIT MESSAGE
#   pre-push    - publication plumbing (remote owner / author / visibility)
#                 plus messages in the push range
set -euo pipefail
repo_root="$(git rev-parse --show-toplevel)"
for hook in pre-commit commit-msg pre-push; do
  chmod +x "$repo_root/githooks/$hook"
done
git -C "$repo_root" config core.hooksPath githooks
echo "core.hooksPath -> githooks (pre-commit, commit-msg, pre-push active)"
echo "Next: provide a denylist via \$SLUICE_FORBIDDEN_IDENTIFIERS or"
echo "      .identifiers-denylist.local (gitignored)."
echo "      Declare publication plumbing in publication.toml (see that file)."
