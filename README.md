# GitHub Productivity Report Generator

## Overview
This Python project generates a detailed productivity report for a GitHub user in a specific repository. The report includes metrics for pull requests, issues, commits, and contributions over a specified time frame. It also estimates the time spent based on code changes and provides insights into the quality of commit messages.


---

## Features
- **Pull Request Analysis**:
  - Tracks PR creation, merging, and closing.
  - Analyzes files changed, lines added/deleted, and comments on PRs.
  - Estimates time spent on PRs based on code changes.
  - Summarizes test and documentation file changes.

- **Issue Tracking**:
  - Tracks issues created and assigned to the user.
  - Includes details on state, labels, milestones, and comments.
  - Calculates average time to close issues.

- **Commit Quality**:
  - Evaluates commit messages for actionable formatting and issue references.
  - Tracks the number of commits referencing issues.

- **Data Caching**:
  - Uses Redis for efficient caching of GitHub API data.
  - Supports cache flushing to refresh data.

- **Markdown Report Generation**:
  - Produces a detailed Markdown report with metrics and insights.
  - Provides tables and summaries for easy reference.

---

## Prerequisites
- **Python 3.11+**
- **Dependencies**:
  - `PyGithub`: For GitHub API interactions.
  - `redis`: For caching.
  - `concurrent.futures`: For parallel data fetching.
  - `argparse`, `datetime`, `json`, `re`, `time`: Standard Python libraries.

- **Environment**:
  - Redis server running on `localhost:6379`.
  - A valid GitHub personal access token set as an environment variable (`GITHUB_TOKEN`).

---

## Installation

### System Deps

```
sudo apt update
sudo apt install redis-server
sudo systemctl start redis
```

### Cloning and Installation

```bash
git clone https://github.com/your-username/github-productivity-report.git
cd github-productivity-report
python3 -m venv venv
source venv/bin/activate
pip install PyGithub redis
```

## Usage

```
export GITHUB_TOKEN="your_personal_access_token"
python3 csgithubreport.py john-doe my-org/my-repo -t 30d
```


### Flushing Cache

To clear the Redis cache and refresh data, run the script with the `--flush` flag:
```bash
python3 csgithubreport.py john-doe my-org/my-repo --flush
```
or `redis-cli flushall`


## Output
The script generates a Markdown report in the format `<username>.<repo-name>.md`. The report includes:

- PR summaries and details.
- Issue summaries and details.
- Commit statistics and quality analysis.
- Test and documentation file metrics.
- Total estimated hours spent on contributions.

## Logging
All activities and errors are logged in `github_kpi.log`.


> NOTE: this project is built with the help of AI technologies
