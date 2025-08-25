#!/bin/sh
# POSIX-only; loud logs; scans user + org Projects; includes Draft items & People fields.

# ===== UI =====
if [ -t 1 ] && [ -z "${NO_COLOR}" ]; then
  BOLD="$(printf '\033[1m')"; DIM="$(printf '\033[2m')"
  RED="$(printf '\033[31m')"; YEL="$(printf '\033[33m')"; GRN="$(printf '\033[32m')"
  BLU="$(printf '\033[34m')"; CYAN="$(printf '\033[36m')"; NC="$(printf '\033[0m')"
else
  BOLD=""; DIM=""; RED=""; YEL=""; GRN=""; BLU=""; CYAN=""; NC=""
fi
info() { printf "%sℹ %s%s%s\n" "$CYAN" "$DIM" "$*" "$NC"; }
step() { printf "%s▶%s %s%s%s\n" "$BLU" "$NC" "$BOLD" "$*" "$NC"; }
ok()  { printf "%s✓%s %s\n" "$GRN" "$NC" "$*"; }
warn(){ printf "%s⚠ %s%s%s\n" "$YEL" "$*" "$NC" ""; }
err() { printf "%s✗ %s%s%s\n" "$RED" "$*" "$NC" "" 1>&2; }
die() { err "$1"; exit 1; }

# ===== Defaults =====
DATE="$(date +%F)"
FIELD_REGEX="start"
ME=""
JSON_OUT=0
LIST_FIELDS=0
DEBUG=0

usage() {
  cat <<EOF
Usage: $(basename "$0") [--date YYYY-MM-DD] [--field-regex REGEX] [--me LOGIN] [--json] [--list-fields] [--debug]
Shows items assigned to you where a DATE field (name ~ REGEX, default "start") has value <= DATE (default: today).
Includes Draft items and People-field assignments.
EOF
}

# ===== Args =====
while [ $# -gt 0 ]; do
  case "$1" in
    --date) DATE="$2"; shift 2 ;;
    --field-regex) FIELD_REGEX="$2"; shift 2 ;;
    --me) ME="$2"; shift 2 ;;
    --json) JSON_OUT=1; shift ;;
    --list-fields) LIST_FIELDS=1; shift ;;
    --debug) DEBUG=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "Unknown arg: $1" ;;
  esac
done

# ===== Pre-flight =====
step "Checking dependencies"
command -v gh >/dev/null 2>&1 || die "gh not found"
command -v jq >/dev/null 2>&1 || die "jq not found"
ok "Dependencies found: gh, jq"

step "Checking GitHub auth status"
gh auth status -h github.com >/dev/null 2>&1 || die "Not logged in to github.com. Run: gh auth login"
ok "gh auth status: logged in"

step "Who am I?"
if [ -z "$ME" ]; then
  ME="$(gh api user --jq .login 2>/dev/null || true)"
  [ -z "$ME" ] && die "Could not determine your login via gh api user"
fi
ok "Using login: ${BOLD}$ME${NC}"

step "Checking token scopes (needs: project, repo, read:org)"
SCOPES="$(gh api -i user 2>/dev/null | grep -i '^x-oauth-scopes:' | cut -d':' -f2- | tr -d '\r' | tr '[:upper:]' '[:lower:]' | sed 's/^ *//')"
[ -n "$SCOPES" ] && info "Token scopes: $SCOPES" || warn "Could not read scopes header; continuing."
ok "Scopes check complete"

step "Checking GraphQL rate limit"
RL="$(gh api rate_limit 2>/dev/null || true)"
if [ -n "$RL" ]; then
  REMAIN="$(printf "%s" "$RL" | jq -r '.resources.graphql.remaining // empty')"
  [ -n "$REMAIN" ] && ok "GraphQL remaining: $REMAIN" || warn "Could not read GraphQL remaining"
else
  warn "rate_limit call failed; continuing."
fi

# ===== Temp files for queries =====
tmp() { mktemp 2>/dev/null || mktemp -t tmp; }

Q_LIST_ORGS="$(tmp)"
cat >"$Q_LIST_ORGS" <<'GQL'
query { viewer { organizations(first:100) { nodes { login } } } }
GQL

Q_LIST_ORG_PROJECTS="$(tmp)"
cat >"$Q_LIST_ORG_PROJECTS" <<'GQL'
query($login:String!) {
  organization(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { number title url closed }
    }
  }
}
GQL

Q_LIST_USER_PROJECTS="$(tmp)"
cat >"$Q_LIST_USER_PROJECTS" <<'GQL'
query($login:String!) {
  user(login:$login){
    projectsV2(first:50, orderBy:{field:UPDATED_AT,direction:DESC}) {
      nodes { number title url closed }
    }
  }
}
GQL

# NOTE: critical fix — `field { name }` on a union is illegal.
# You MUST select a specific type: `... on ProjectV2FieldCommon { name }`.
Q_SCAN_ORG="$(tmp)"
cat >"$Q_SCAN_ORG" <<'GQL'
query($org:String!, $number:Int!, $after:String) {
  organization(login:$org){
    projectV2(number:$number){
      items(first:100, after:$after){
        pageInfo{ hasNextPage endCursor }
        nodes{
          content{
            __typename
            ... on DraftIssue { title }
            ... on Issue {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
            ... on PullRequest {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
          }
          fieldValues(first:50){
            nodes{
              __typename
              ... on ProjectV2ItemFieldDateValue {
                date
                field { ... on ProjectV2FieldCommon { name } }
              }
              ... on ProjectV2ItemFieldUserValue {
                users(first:50){ nodes{ login } }
                field { ... on ProjectV2FieldCommon { name } }
              }
            }
          }
          project{ title url }
        }
      }
    }
  }
}
GQL

Q_SCAN_USER="$(tmp)"
cat >"$Q_SCAN_USER" <<'GQL'
query($login:String!, $number:Int!, $after:String) {
  user(login:$login){
    projectV2(number:$number){
      items(first:100, after:$after){
        pageInfo{ hasNextPage endCursor }
        nodes{
          content{
            __typename
            ... on DraftIssue { title }
            ... on Issue {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
            ... on PullRequest {
              title url repository{ nameWithOwner }
              assignees(first:50){ nodes{ login } }
            }
          }
          fieldValues(first:50){
            nodes{
              __typename
              ... on ProjectV2ItemFieldDateValue {
                date
                field { ... on ProjectV2FieldCommon { name } }
              }
              ... on ProjectV2ItemFieldUserValue {
                users(first:50){ nodes{ login } }
                field { ... on ProjectV2FieldCommon { name } }
              }
            }
          }
          project{ title url }
        }
      }
    }
  }
}
GQL

cleanup() {
  rm -f "$Q_LIST_ORGS" "$Q_LIST_ORG_PROJECTS" "$Q_LIST_USER_PROJECTS" "$Q_SCAN_ORG" "$Q_SCAN_USER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# ===== Owners =====
OWNERS_FILE="$(tmp)"
echo "user:$ME" > "$OWNERS_FILE"

ORG_LOGINS="$(gh api graphql -F query=@$Q_LIST_ORGS --jq '.data.viewer.organizations.nodes[].login' 2>/dev/null || true)"
if [ -n "$ORG_LOGINS" ]; then
  echo "$ORG_LOGINS" | while IFS= read -r org; do
    [ -n "$org" ] && printf "org:%s\n" "$org"
  done >> "$OWNERS_FILE"
fi
[ "$DEBUG" -eq 1 ] && { step "Owners to scan"; cat "$OWNERS_FILE"; }

# ===== Helpers =====
list_projects_for() {
  TYPE="$1"; LOGIN="$2"
  if [ "$TYPE" = "org" ]; then
    gh api graphql -F query=@$Q_LIST_ORG_PROJECTS -F login="$LOGIN" \
      --jq '.data.organization.projectsV2.nodes[] | select(.closed==false) | "\(.number)\t\(.title)\t\(.url)"' 2>/dev/null
  else
    gh api graphql -F query=@$Q_LIST_USER_PROJECTS -F login="$LOGIN" \
      --jq '.data.user.projectsV2.nodes[] | select(.closed==false) | "\(.number)\t\(.title)\t\(.url)"' 2>/dev/null
  fi
}

scan_project() {
  TYPE="$1"; LOGIN="$2"; NUMBER="$3"; PTITLE="$4"; PURL="$5"
  [ "$DEBUG" -eq 1 ] && info "Scanning $TYPE:$LOGIN project #$NUMBER '$PTITLE'"
  AFTER=""
  while :; do
    if [ "$TYPE" = "org" ]; then
      RESP="$(gh api graphql -F query=@$Q_SCAN_ORG -F org="$LOGIN" -F number="$NUMBER" ${AFTER:+-F after="$AFTER"})"
    else
      RESP="$(gh api graphql -F query=@$Q_SCAN_USER -F login="$LOGIN" -F number="$NUMBER" ${AFTER:+-F after="$AFTER"})"
    fi
    printf "%s" "$RESP" | jq -c --arg me "$ME" --arg date "$DATE" --arg field_regex "$FIELD_REGEX" '
      def assigned_to_me:
        ( .content.__typename == "Issue" or .content.__typename == "PullRequest" )
        and ( [ .content.assignees.nodes[]?.login ] | index($me) != null )
        or ( [ .fieldValues.nodes[]?
                | select(.__typename=="ProjectV2ItemFieldUserValue")
                | .users.nodes[]?.login
            ] | index($me) != null );

      def start_match:
        [ .fieldValues.nodes[]?
          | select(.__typename=="ProjectV2ItemFieldDateValue")
          | select((.field.name // "") | test($field_regex; "i"))
          | select(.date <= $date)
        ][0];

      (.. | .items? | objects | .nodes? // empty)[]? as $it
      | ( $it | assigned_to_me ) as $am
      | ( $it | start_match ) as $sm
      | select($am and ($sm != null))
      | {
          project: ($it.project.title // ""),
          project_url: ($it.project.url // ""),
          start_field: $sm.field.name,
          start_date: $sm.date,
          title: ( $it.content.title // "(Draft item)" ),
          url:   ( $it.content.url   // ($it.project.url // "") ),
          repo:  ( $it.content.repository.nameWithOwner // null )
        }
    ' 2>/dev/null >> "$RESULTS_LINES"

    HAS_NEXT="$(printf "%s" "$RESP" | jq -r '.. | .items? | objects | .pageInfo? // empty | .hasNextPage // empty' 2>/dev/null | tail -n1)"
    AFTER="$(printf "%s" "$RESP" | jq -r '.. | .items? | objects | .pageInfo? // empty | .endCursor // empty' 2>/dev/null | tail -n1)"
    [ "$HAS_NEXT" = "true" ] || break
  done
}

# ===== Enumerate & scan =====
RESULTS_LINES="$(tmp)"; : > "$RESULTS_LINES"

step "Enumerating Projects"
while IFS=: read -r OTYPE OLOGIN; do
  [ -z "$OTYPE" ] && continue
  [ -z "$OLOGIN" ] && continue
  step "Listing projects for $OTYPE:$OLOGIN"
  PROJS="$(list_projects_for "$OTYPE" "$OLOGIN")"
  if [ -z "$PROJS" ]; then
    warn "No open projects for $OTYPE:$OLOGIN"
    continue
  fi
  echo "$PROJS" | while IFS="$(printf '\t')" read -r NUMBER PTITLE PURL; do
    [ -z "$NUMBER" ] && continue
    scan_project "$OTYPE" "$OLOGIN" "$NUMBER" "$PTITLE" "$PURL"
  done
done < "$OWNERS_FILE"

# ===== Collate =====
if [ ! -s "$RESULTS_LINES" ]; then
  [ "$LIST_FIELDS" -eq 1 ] && { warn "No items found to infer fields."; exit 0; }
  ok "Matched items: 0"
  warn "No items match field ~ /$FIELD_REGEX/i and date <= $DATE."
  info "Tips: set --field-regex to your actual Start field (e.g., '^Start( date)?$' or 'begin|kickoff')."
  exit 0
fi

RESULTS_JSON="$(jq -s 'sort_by(.project, .start_date, .repo, .title)' "$RESULTS_LINES")"
COUNT="$(printf "%s" "$RESULTS_JSON" | jq 'length')"
ok "Matched items: $COUNT"

if [ "$LIST_FIELDS" -eq 1 ]; then
  step "Listing distinct DATE field names"
  printf "%s" "$RESULTS_JSON" | jq -r '.[].start_field' | sort -u
  exit 0
fi

if [ "$JSON_OUT" -eq 1 ]; then
  printf "%s\n" "$RESULTS_JSON"
  exit 0
fi

# ===== Pretty print =====
step "Rendering results"

# Build one stream: "## <project>" header, then TSV table lines, then a blank line between projects.
TMP_LINES="$(mktemp 2>/dev/null || mktemp -t tmp)"
printf "%s" "$RESULTS_JSON" | jq -r '
  group_by(.project)[]
  | "## " + (.[0].project // "(No project title)"),
    "DATE\tFIELD\tTITLE\tREPO\tURL",
    ( .[] | [ .start_date, .start_field, .title, (.repo // "-"), .url ] | @tsv ),
    ""
' > "$TMP_LINES"

# Compute terminal width and sensible column widths with truncation.
COLS="${COLUMNS:-}"
[ -z "$COLS" ] && COLS="$(tput cols 2>/dev/null || true)"
[ -z "$COLS" ] && COLS="$(stty size 2>/dev/null | awk '{print $2}' || true)"
[ -z "$COLS" ] && COLS=120

W_DATE=10     #  YYYY-MM-DD
W_FIELD=22
W_REPO=28
W_URL=40
SPACE_PAD=2   # spaces between columns

sum_fixed=$(( W_DATE + W_FIELD + W_REPO + W_URL + (SPACE_PAD*4) ))
W_TITLE=$(( COLS - sum_fixed ))
# Minimums; if terminal is narrow, steal from URL/REPO/FIELD to keep at least this much for TITLE
MIN_TITLE=18; MIN_FIELD=14; MIN_REPO=14; MIN_URL=20

# Rebalance if needed to guarantee at least MIN_TITLE for title.
rebalance() {
  need=$(( MIN_TITLE - W_TITLE ))
  [ $need -le 0 ] && return 0
  # Steal from URL, then REPO, then FIELD down to their minimums
  take=$need
  avail=$(( W_URL - MIN_URL ))
  if [ $avail -gt 0 ]; then
    d=$([ $avail -ge $take ] && echo $take || echo $avail)
    W_URL=$(( W_URL - d )); take=$(( take - d ))
  fi
  [ $take -le 0 ] || {
    avail=$(( W_REPO - MIN_REPO ))
    if [ $avail -gt 0 ]; then
      d=$([ $avail -ge $take ] && echo $take || echo $avail)
      W_REPO=$(( W_REPO - d )); take=$(( take - d ))
    fi
  }
  [ $take -le 0 ] || {
    avail=$(( W_FIELD - MIN_FIELD ))
    if [ $avail -gt 0 ]; then
      d=$([ $avail -ge $take ] && echo $take || echo $avail)
      W_FIELD=$(( W_FIELD - d )); take=$(( take - d ))
    fi
  }
  W_TITLE=$(( COLS - (W_DATE + W_FIELD + W_REPO + W_URL + (SPACE_PAD*4)) ))
}
rebalance

# AWK pretty-printer: fixed widths + ellipses, no BSD column(1) dependency.
AWK_FMT='
BEGIN{
  FS="\t"; OFS=sprintf("%*s", sp,"");  # sp spaces between cols
  # header underline uses simple dashes
  dash_date = sprintf("%"wd"s",""); gsub(/ /,"-",dash_date)
  dash_field= sprintf("%"wf"s",""); gsub(/ /,"-",dash_field)
  dash_title= sprintf("%"wt"s",""); gsub(/ /,"-",dash_title)
  dash_repo = sprintf("%"wr"s",""); gsub(/ /,"-",dash_repo)
  dash_url  = sprintf("%"wu"s",""); gsub(/ /,"-",dash_url)
}
function trunc(s,w,   ell) {
  ell = "…"
  if (w<=0) return ""
  if (length(s) <= w) return s
  if (w<=1) return substr(s,1,w)
  return substr(s,1,w-1) ell
}
function fmt(d,f,t,r,u){
  printf "%-"wd"s%s%-"wf"s%s%-"wt"s%s%-"wr"s%s%-"wu"s\n",
         d,OFS,f,OFS,t,OFS,r,OFS,u
}
{
  if ($0 ~ /^## /) {
    # Flush pending table header underline state
    print hdr_line
    hdr_line=""
    print proj_bold $0 proj_norm
    next
  }
  if ($0=="") {
    # End of table block: print underline after header (if any), then blank
    if (printed_header==1) {
      # underline already printed below header; nothing here
      printed_header=0
    }
    print ""
    next
  }
  # Table lines (TSV)
  if ($0 ~ /^DATE\tFIELD\tTITLE\tREPO\tURL$/) {
    d="DATE"; f="FIELD"; t="TITLE"; r="REPO"; u="URL"
    fmt(d,f,t,r,u)
    print trunc(dash_date,wd) OFS trunc(dash_field,wf) OFS trunc(dash_title,wt) OFS trunc(dash_repo,wr) OFS trunc(dash_url,wu)
    printed_header=1
  } else {
    split($0,a,"\t")
    d=trunc(a[1],wd); f=trunc(a[2],wf); t=trunc(a[3],wt); r=trunc(a[4],wr); u=trunc(a[5],wu)
    fmt(d,f,t,r,u)
  }
}
'

proj_bold="$BOLD"; proj_norm="$NC"

awk \
  -v wd="$W_DATE" -v wf="$W_FIELD" -v wt="$W_TITLE" -v wr="$W_REPO" -v wu="$W_URL" \
  -v sp="$SPACE_PAD" \
  -v proj_bold="$proj_bold" -v proj_norm="$proj_norm" \
  "$AWK_FMT" < "$TMP_LINES"

ok "Done."

