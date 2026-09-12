#!/usr/bin/env fish
# Append text to every commit subject in the range (BEGIN, END].
#
# Usage: append-commit-subjects.fish <COMMIT_BEGIN> <COMMIT_END> <APPENDIX>
# Example: append-commit-subjects.fish HEAD~2 HEAD ' hello'
#
# Requires ``par`` (``brew install par`` on macOS). Unlike ``fmt``, it
# optimizes paragraph line breaks instead of greedily filling each line.

if test (count $argv) -ge 1; and test $argv[1] = --amend-current
    if not set -q APPENDIX
        echo 'APPENDIX must be set in the environment.' >&2
        exit 1
    end
    set -l subject (git log -1 --format=%s)
    or exit 1
    set -l body (git log -1 --format=%b | string collect)
    set -l formatted_body (printf '%s\n' $body | par 72)
    printf '%s%s\n\n%s\n' $subject $APPENDIX $formatted_body |
        git commit --amend --no-verify --file -
    exit 0
end

if not command -sq par
    echo 'The par command is required; install it with: brew install par' >&2
    exit 2
end

if test (count $argv) -ne 3
    echo "Usage: $(status filename) <COMMIT_BEGIN> <COMMIT_END> <APPENDIX>" >&2
    exit 2
end

set -l begin (git rev-parse --verify "$argv[1]^{commit}")
or exit 1
set -l end (git rev-parse --verify "$argv[2]^{commit}")
or exit 1
set -l appendix $argv[3]
set -l head (git rev-parse HEAD)
or exit 1

if test $end != $head
    echo 'COMMIT_END must be HEAD; check out the branch tip to rewrite it.' >&2
    exit 2
end

if not git merge-base --is-ancestor $begin $end
    echo 'COMMIT_BEGIN must be an ancestor of COMMIT_END.' >&2
    exit 2
end

# if test -n "$(git status --porcelain)"
#     echo 'Working tree must be clean before rewriting history.' >&2
#     exit 2
# end

set -l script_path (realpath (status filename))
set -l todo_editor (mktemp)
or exit 1
trap "rm -f $todo_editor" EXIT

set -l escaped_path (string replace -a "'" "'\\''" -- $script_path)
printf '%s\n' \
    '#!/usr/bin/env bash' \
    'set -euo pipefail' \
    "awk -v command=\"exec '$escaped_path' --amend-current\" '" \
    '    { print }' \
    '    $1 == "pick" { print command }' \
    "' \"\$1\" > \"\$1.tmp\"" \
    'mv "$1.tmp" "$1"' >"$todo_editor"

chmod +x $todo_editor

env APPENDIX=$appendix GIT_SEQUENCE_EDITOR=$todo_editor git rebase -i --onto $begin $begin $end
