#!/usr/bin/env bash
# Append text to every commit subject in the range (BEGIN, END].
#
# Usage: utils/append-commit-subjects.sh <COMMIT_BEGIN> <COMMIT_END> <APPENDIX>
# Example: utils/append-commit-subjects.sh HEAD~2 HEAD ' hello'
#
# Requires ``par`` (``brew install par`` on macOS). Unlike ``fmt``, it
# optimizes paragraph line breaks instead of greedily filling each line.

set -euo pipefail

if [[ "${1:-}" == '--amend-current' ]]; then
    subject=$(git log -1 --format=%s)
    body=$(git log -1 --format=%b)
    formatted_body=$(printf '%s\n' "$body" | par 72)
    printf '%s%s\n\n%s\n' "$subject" "$APPENDIX" "$formatted_body" |
        git commit --amend --no-verify --file -
    exit 0
fi

if ! command -v par >/dev/null; then
    echo 'The par command is required; install it with: brew install par' >&2
    exit 2
fi

if [[ $# -ne 3 ]]; then
    echo "Usage: $0 <COMMIT_BEGIN> <COMMIT_END> <APPENDIX>" >&2
    exit 2
fi

begin=$(git rev-parse --verify "$1^{commit}")
end=$(git rev-parse --verify "$2^{commit}")
appendix=$3
head=$(git rev-parse HEAD)

if [[ $end != "$head" ]]; then
    echo 'COMMIT_END must be HEAD; check out the branch tip to rewrite it.' >&2
    exit 2
fi

if ! git merge-base --is-ancestor "$begin" "$end"; then
    echo 'COMMIT_BEGIN must be an ancestor of COMMIT_END.' >&2
    exit 2
fi

#if [[ -n $(git status --porcelain) ]]; then
#    echo 'Working tree must be clean before rewriting history.' >&2
#    exit 2
#fi

script_path=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/$(basename -- "${BASH_SOURCE[0]}")
todo_editor=$(mktemp)
trap 'rm -f "$todo_editor"' EXIT

cat >"$todo_editor" <<EOF
#!/usr/bin/env bash
set -euo pipefail
awk -v command="exec '$script_path' --amend-current" '
    { print }
    \$1 == "pick" { print command }
' "\$1" > "\$1.tmp"
mv "\$1.tmp" "\$1"
EOF
chmod +x "$todo_editor"

APPENDIX=$appendix GIT_SEQUENCE_EDITOR=$todo_editor git rebase -i --onto "$begin" "$begin" "$end"
