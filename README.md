# GitHub Projects (v2) Terminal Task Viewer + Work Timers

Run GitHub Projects like a command center, not a chore list. This terminal-first companion keeps you in flow, laser-focused on the next deliverable, and ready with executive updates in minutes instead of hours.

Built for maintainers, busy teammates, and solo builders who want supercharged task management without another web dashboard.

## Why it saves hours

- Keyboard-native UI that loads instantly and adapts to any terminal width, so you never break focus
- Local SQLite cache that mirrors your Projects (v2) data for fast filtering, offline browsing, and zero rate-limit panic
- Per-task work timers with concurrent tracking to capture every context switch automatically
- Time intelligence that rolls up daily/weekly/monthly/yearly aggregates for standups, retros, and billing
- One-page PDF and structured JSON exports that make sharing progress as easy as hitting `Z`

## Superpowers at a glance

- Slice projects, assignees, and focus windows on the fly to see what truly needs attention
- Lightning-fast search, sortable columns, and configurable date fields tuned for project leads
- Integrated discovery to pull the right Projects (org or user) into your workspace instantly
- Tunable themes and layouts so the UI matches your environment, dark-night terminal included

Ready to reclaim your GitHub task list? Jump in with the quick install below and start timing your work within minutes.


## Install

Requires Python 3.9+. In just a few commands you'll be ready to sync projects and start timing your work.

1) Create a virtualenv (recommended) and install deps:

```
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```

2) Auth token

Create a GitHub personal access token with the necessary scopes:

- Recommended: classic token (Settings → Developer settings → Personal access tokens → Tokens (classic) → Generate new token)
- Scopes to enable:
  - `repo` (full repository access)
  - `project` (access Projects v2 via GraphQL)
  - `user` (read user profile details for filtering/assignment checks)
  - `notifications` (optional, recommended if you later expand features)
  - `gist` (optional, only needed if exporting to gists)
  - `admin:org` (only if you need to access private org projects; otherwise use `read:org`)

Note: if you use fine-grained tokens, make sure to grant access to the organizations and repositories you need, and include Projects and Repository permissions analogous to the above classic scopes.

Set the token in your environment as `GITHUB_TOKEN`, or place it into a local `.env` file next to the script:

```
GITHUB_TOKEN=ghp_your_token_here
```


## Configuration

Tailor what the viewer pulls down so every fetch reflects the projects you care about.

Create `config_gh_task.yml` (or use your own path and pass `--config`):

```
# Minimal example
user: your-github-login
# Either use regex for the date fields you want to treat as the "start"/"focus" filters
# date_field_regex: "^(Start date|Due date|Target date)$"
# Or list them explicitly
date_field_names: ["Start date", "Due date", "Target date"]

projects:
  # Track all open Projects v2 in an org
  - org: your-org
    numbers: all
  # Track specific user projects by number
  - user: your-github-login
    numbers: [1, 4]
```

Notes
- `user` is your GitHub login (used to detect "assigned to me" via issue/PR assignees or People field).
- For `projects`, set either `org:` or `user:` per entry, and use `numbers: all` to discover open Projects v2 or a list of integers for specific project numbers.
- The app auto-detects date fields by name using your regex or list.

To change the user in the config, just update the `user: ...` value and restart the app.


## Usage

Spin it up whenever you need instant clarity on what's next.

Basic run (TUI):

```
python gh_task_viewer.py --config config_gh_task.yml --db gh_tasks.db
```

Discover open Projects v2 for each owner:

```
python gh_task_viewer.py --config config_gh_task.yml --discover
```

Non-interactive summary (quick check):

```
python gh_task_viewer.py --config config_gh_task.yml --no-ui
```


## UI Cheatsheet

Learn the keystrokes that keep you in the flow.

Navigation
- `j / k`, arrow keys: move
- `gg / G`: jump to top / bottom
- `h / l`, arrow keys: horizontal scroll
- `Enter`: open/close detail pane
- `/`: live search (type, Enter to apply, Esc to cancel)
- `s`: toggle sort Project/Date

Filters
- `p`: cycle project filter
- `P`: clear project filter
- `d`: toggle hide-done
- `N`: toggle hide tasks with no date
- `F`: set Date <= filter (YYYY-MM-DD)
- `t / a`: show today only / show all
- `T`: set the selected task's focus day to today
- `Y / y`: move the selected task's focus day forward/backward by one day

Cache & fetch
- `u`: fetch from GitHub and update local cache
- `U`: toggle include-unassigned (press `u` after toggling to refetch)

Timers & reports
- `W`: toggle work timer for the selected task (multiple can run)
- `R`: open the Timer Report (daily/weekly/monthly aggregates)
- `X`: export a JSON report (quick)
- `Z`: export a one-page PDF report (quick; requires `reportlab`)

General
- `?`: help
- `q` or `Esc`: close detail/help/report, or quit


## Themes

Personalize the control center to match your environment, from midnight terminals to sunlit monitors.

- Press `Shift+1` through `Shift+0` to switch among the loaded theme presets.
- Presets live as YAML files in the `themes/` folder; use `themes/default.yaml` or `themes/solarized_light.yaml` as starting points when authoring new ones.
- Each preset defines both color rules (`style`) and a layout (`vertical`/`horizontal`). The first preset is used by default unless a persisted theme index is restored from the UI state file.
- The viewer saves your last-selected preset automatically so it is restored on the next launch.
- Layouts can be tuned per theme: set `layout: vertical`/`horizontal` directly, or provide a mapping such as
  ```yaml
  layout:
    orientation: horizontal
    stats_height: 14  # rows for the overview panel when it sits below the table
  ```
  For vertical layouts you can set `stats_width` to widen or narrow the sidebar.


## What You See

Get situational awareness at a glance.

- Dynamic table width that adapts to your terminal size
- Columns: Focus Date, Start Date, Status, Time, Title, Project
- `Time` column shows `mm:ss|H:MM` = current run | total per task
- Running timers are marked with `⏱` and rendered in cyan across the entire row
- Rows are color-coded by focus date: today in bold red, past dates a softer red, and future dates rotate through a palette so each day stands out (status-specific colors still take precedence for "In Progress" or "Waiting")
- Top bar shows a live Now/Task/Project timers snapshot
- Right panel lists per-project stats and total time


## Work Timers

Let the app capture your focus sessions automatically while you stay heads-down.

- Timers are stored in SQLite (`work_sessions` table) with start/stop times.
- You can run multiple timers at once (e.g., context switching across tasks).
- Toggle a timer with `W` on the selected row.
- Timers are keyed by the task `url`. Tasks without a URL cannot be tracked.


## Reports & Exports

Progress updates stop being a chore when the data is already packaged for you.

Timer Report (`R`)
- Shows a summary snapshot and aggregates by Day/Week/Month.
- Quick view panel sums across the last 14 days / ~12 weeks / ~12 months.

PDF Export
- One-page portrait PDF with:
  - Header (username and generated date)
  - 4 mini donut charts (D/W/M/Y) showing percentage of time per project for each window
  - Per Project table: D/W/M/Y in H:MM, sorted by yearly time
  - Per Task table: D/W/M/Y in H:MM, sorted by yearly time
- From CLI:

```
python gh_task_viewer.py --config config_gh_task.yml --export-pdf report.pdf
# Or render from an already exported JSON
python gh_task_viewer.py --config config_gh_task.yml --export-pdf report.pdf --pdf-from-json report.json
```

JSON Export

```
python gh_task_viewer.py \
  --config config_gh_task.yml \
  --export-report report.json \
  --export-since-days 90 \
  --export-granularity all \
  --export-scope all \
  [--export-project "Project Title"] [--export-task-url "https://github.com/org/repo/issues/123"]
```

## Tests

Run the automated suite with pytest (activate your virtualenv first if you created one):

```
pytest
```

While iterating, scope pytest to an individual module:

```
pytest tests/test_fetch_pipeline.py
```

Payload includes:
- overall totals per period (day/week/month)
- per-project totals (window)
- per-project per-period buckets
- optional selected project/task per-period buckets
- per-task totals (window) and a task title map for labels


## Database

All state lives in one tidy SQLite file, so you stay portable and in control.

- SQLite path is given by `--db` (defaults to `~/.gh_tasks.db`).
- Tables
  - `tasks`: cached entries from GitHub Projects v2
  - `work_sessions`: per-task timer sessions (start/end timestamps)
- Safe migrations are handled automatically on start.


## Handling Rate Limits

The fetcher uses backoff and handles GitHub GraphQL `RATE_LIMITED` gracefully. If it hits rate limits, it keeps partial results and the UI shows a friendly message; fetch again later with `u`.


## Troubleshooting

Hit a snag? These cover the usual suspects.

- No data on first run? Press `u` to fetch from GitHub.
- Missing token? Set `GITHUB_TOKEN` env var or `.env` file.
- PDF export fails? Install ReportLab: `pip install reportlab`.
- Tables wrap/scroll oddly? Reduce font size or widen the terminal; columns adapt automatically.
