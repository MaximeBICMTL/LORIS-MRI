#!/bin/sh
set -eu

if [ "$#" -lt 4 ] || [ "$#" -gt 6 ]; then
    echo "usage: $0 <upstream-main> <release-base> <cbig-major> <cbig-main> [feature-tip cbig-feature]" >&2
    exit 2
fi

upstream_main=$1
release_base=$2
cbig_major=$3
cbig_main=$4
feature_tip=${5-}
cbig_feature=${6-}

if [ -n "$feature_tip" ] && [ -z "$cbig_feature" ]; then
    echo "feature-tip and cbig-feature must be supplied together" >&2
    exit 2
fi

for ref in "$upstream_main" "$release_base" "$cbig_major" "$cbig_main" $feature_tip $cbig_feature; do
    git rev-parse --verify --quiet "$ref^{commit}" >/dev/null || {
        echo "missing commit ref: $ref" >&2
        exit 1
    }
done

heading() { printf '\n%s\n' "$1"; }

ancestor_result() {
    if git merge-base --is-ancestor "$1" "$2"; then result=yes; else result=no; fi
    printf '%-30s ancestor of %-30s %s\n' "$1" "$2" "$result"
}

show_tip() {
    printf '%-30s ' "$1"
    git show -s --format='%H %s' "$1"
}

heading "WORKTREE"
git status --short --branch

heading "TIPS"
for ref in "$upstream_main" "$release_base" "$cbig_major" "$cbig_main" $feature_tip $cbig_feature; do
    show_tip "$ref"
done

heading "C-BIG VERSION"
git show "$cbig_major:VERSION" 2>/dev/null || echo "VERSION missing"

heading "RELEASE TO UPSTREAM MAIN"
git log --reverse --topo-order --format='%h %p %s' "$release_base..$upstream_main"

heading "C-BIG MAJOR TO C-BIG MAIN"
git log --reverse --topo-order --format='%h %p %s' "$cbig_major..$cbig_main"

heading "FORBIDDEN C-BIG ANCESTRY"
for base in "$upstream_main" "$release_base"; do
    ancestor_result "$base" "$cbig_major"
    ancestor_result "$base" "$cbig_main"
    if [ -n "$cbig_feature" ]; then ancestor_result "$base" "$cbig_feature"; fi
done

heading "C-BIG MAIN OUT-OF-PRIMARY-SCOPE PATHS"
git diff --name-only "$cbig_major..$cbig_main" | awk '
    $0 == "pyproject.toml" { next }
    $0 ~ /^python\// { next }
    $0 ~ /^\.github\// { next }
    $0 ~ /^test\// { next }
    { print }
'

heading "C-BIG OVERRIDE MARKERS"
for ref in "$cbig_major" "$cbig_main" $cbig_feature; do
    starts=$(git grep -h -F '# C-BIG OVERRIDE START' "$ref" -- python 2>/dev/null | wc -l | tr -d ' ')
    ends=$(git grep -h -F '# C-BIG OVERRIDE END' "$ref" -- python 2>/dev/null | wc -l | tr -d ' ')
    printf '%-30s START=%s END=%s' "$ref" "$starts" "$ends"
    if [ "$starts" = "$ends" ]; then echo " balanced"; else echo " UNBALANCED"; fi
done

heading "STALE LEGACY PYTHON ENTRY-POINT CALLERS"
git grep -n -E 'python/(scripts/)?(run_dicom_archive_loader|run_nifti_insertion|run_dicom_archive_validation|run_push_imaging_files_to_s3_pipeline)\.py' "$cbig_main" -- '*.pl' '*.pm' '*.md' 2>/dev/null || true

if [ -n "$feature_tip" ]; then
    heading "FEATURE MERGES OUTSIDE UPSTREAM MAIN"
    git log --reverse --topo-order --merges --format='%H%nparents: %P%nsubject: %s%n' "$upstream_main..$feature_tip"

    heading "FEATURE DEPENDENCY REFS"
    git for-each-ref --format='%(refname:short)' refs/heads refs/remotes | while IFS= read -r ref; do
        git merge-base --is-ancestor "$ref" "$feature_tip" || continue
        git merge-base --is-ancestor "$ref" "$upstream_main" && continue
        printf '%-12s %s\n' "$(git rev-parse --short "$ref")" "$ref"
    done

    heading "C-BIG FEATURE COMMITS"
    git log --reverse --topo-order --format='%h %p %s' "$cbig_main..$cbig_feature"

    heading "DATABASE MODEL PATHS"
    git diff --name-status "$upstream_main..$feature_tip" -- 'python/**/models/**' 'python/lib/db/models/**'
    git diff --name-status "$cbig_main..$cbig_feature" -- 'python/**/models/**' 'python/lib/db/models/**'
fi

heading "DIFF CHECK"
git diff --check "$cbig_major..$cbig_main"
if [ -n "$cbig_feature" ]; then git diff --check "$cbig_main..$cbig_feature"; fi
