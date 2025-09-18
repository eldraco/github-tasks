# GitHub Projects (v2) Terminal Task Viewer + Work Timers

A fast, keyboard-driven TUI to browse GitHub Projects (v2) issues/PRs, filter and search them locally, track per-task work time, and export beautiful one-page PDF reports with time aggregates.

- Terminal-first UI with dynamic column widths
- Works offline from a local SQLite cache
- Per-task timers with multi-task concurrent tracking
- Aggregated time reports (Daily/Weekly/Monthly/Yearly)
- One-page PDF export with charts, plus JSON exports for automation


## Install

Requires Python 3.9+.

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


## What You See

- Dynamic table width that adapts to your terminal size
- Columns: Focus Date, Start Date, Status, Time, Title, Project
- `Time` column shows `mm:ss|H:MM` = current run | total per task
- Running timers are marked with `⏱` and rendered in cyan
- Top bar shows a live Now/Task/Project timers snapshot
- Right panel lists per-project stats and total time


## Work Timers

- Timers are stored in SQLite (`work_sessions` table) with start/stop times.
- You can run multiple timers at once (e.g., context switching across tasks).
- Toggle a timer with `W` on the selected row.
- Timers are keyed by the task `url`. Tasks without a URL cannot be tracked.


## Reports & Exports

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

Payload includes:
- overall totals per period (day/week/month)
- per-project totals (window)
- per-project per-period buckets
- optional selected project/task per-period buckets
- per-task totals (window) and a task title map for labels


## Database

- SQLite path is given by `--db` (defaults to `~/.gh_tasks.db`).
- Tables
  - `tasks`: cached entries from GitHub Projects v2
  - `work_sessions`: per-task timer sessions (start/end timestamps)
- Safe migrations are handled automatically on start.


## Handling Rate Limits

The fetcher uses backoff and handles GitHub GraphQL `RATE_LIMITED` gracefully. If it hits rate limits, it keeps partial results and the UI shows a friendly message; fetch again later with `u`.


## Troubleshooting

- No data on first run? Press `u` to fetch from GitHub.
- Missing token? Set `GITHUB_TOKEN` env var or `.env` file.
- PDF export fails? Install ReportLab: `pip install reportlab`.
- Tables wrap/scroll oddly? Reduce font size or widen the terminal; columns adapt automatically.


## License

This repo contains original code for a terminal viewer; no license is asserted here. If you plan to publish or share, please add an explicit license of your choice.
