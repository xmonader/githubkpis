#!/usr/bin/env python3

import argparse
import os
import sys
import json
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any, List

from github import Github, Repository, PullRequest, Issue
import redis
import concurrent.futures
import logging

# ----------------------------
# Constants
# ----------------------------

DEFAULT_TIME_FRAME = "7d"
CACHE_EXPIRY = 604800  # 7 days in seconds
HOURS_PER_LINE = 0.05  # Estimation: 0.05 hours per line added/deleted

# ----------------------------
# Configure Logging
# ----------------------------

logging.basicConfig(
    filename='github_kpi.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

# ----------------------------
# Caching Helper Functions
# ----------------------------

def cache_key(prefix: str, identifier: str, version: int = 1) -> str:
    """
    Generate a Redis cache key with versioning.
    """
    return f"{prefix}:v{version}:{identifier}"

def get_cached_data(r: redis.Redis, key: str) -> Optional[Dict[str, Any]]:
    """
    Retrieve cached data from Redis.
    """
    cached = r.get(key)
    if cached:
        try:
            data = json.loads(cached)
            if isinstance(data, dict) and 'data' in data:
                logging.debug(f"Cache hit for key: {key}")
                return data
            elif isinstance(data, list):
                # Handle unexpected list format by wrapping it in a dict
                logging.warning(f"Unexpected cache format for key: {key}. Wrapping list in 'data' key.")
                return {'data': data}
            else:
                logging.error(f"Invalid cache format for key: {key}. Expected dict with 'data' key.")
                return None
        except json.JSONDecodeError:
            logging.error(f"JSON decode error for cache key: {key}.")
            return None
    logging.debug(f"Cache miss for key: {key}")
    return None

def set_cache_data(r: redis.Redis, key: str, data: Dict[str, Any]):
    """
    Set data in Redis cache.
    """
    try:
        r.set(key, json.dumps(data), ex=CACHE_EXPIRY)
        logging.debug(f"Cache set for key: {key}")
    except Exception as e:
        logging.error(f"Error setting cache for key {key}: {e}")

def flush_cache(r: redis.Redis):
    """
    Flush all data from Redis cache.
    """
    try:
        r.flushdb()
        logging.info("Redis cache has been flushed.")
    except Exception as e:
        logging.error(f"Error flushing Redis cache: {e}")

def pr_cache_key(repo_full_name: str, pr_number: int) -> str:
    """
    Generate a cache key for a specific pull request.
    """
    return cache_key("pr_data", f"{repo_full_name}:{pr_number}")

def pr_commits_cache_key(repo_full_name: str, pr_number: int) -> str:
    """
    Generate a cache key for commits of a specific PR.
    """
    return cache_key("pr_commits", f"{repo_full_name}:{pr_number}")

def pr_files_cache_key(repo_full_name: str, pr_number: int) -> str:
    """
    Generate a cache key for files of a specific PR.
    """
    return cache_key("pr_files", f"{repo_full_name}:{pr_number}")

def pr_comments_cache_key(repo_full_name: str, pr_number: int) -> str:
    """
    Generate a cache key for comments of a specific PR.
    """
    return cache_key("pr_comments", f"{repo_full_name}:{pr_number}")

def issue_cache_key(repo_full_name: str, issue_number: int, category: str) -> str:
    """
    Generate a cache key for a specific issue.
    """
    return cache_key(f"issue_{category}", f"{repo_full_name}:{issue_number}")

def issue_comments_cache_key(repo_full_name: str, issue_number: int) -> str:
    """
    Generate a cache key for comments of a specific issue.
    """
    return cache_key("issue_comments", f"{repo_full_name}:{issue_number}")

# ----------------------------
# Time Parsing and Clamping
# ----------------------------

def parse_time_frame(time_str: str) -> datetime:
    """
    Parse the time frame string and return the corresponding timezone-aware datetime object in UTC.
    Supports days (d) and months (m).
    """
    match = re.match(r'(\d+)([dm])', time_str)
    if not match:
        raise ValueError("Time frame must be in the format <number>d or <number>m (e.g., 30d, 3m)")
    value, unit = match.groups()
    value = int(value)
    if unit == 'd':
        since = datetime.now(timezone.utc) - timedelta(days=value)
    elif unit == 'm':
        since = datetime.now(timezone.utc) - timedelta(days=30 * value)
    else:
        raise ValueError("Unsupported time unit. Use 'd' for days or 'm' for months.")
    return since

def clamp_datetime(dt: datetime, start: datetime, end: datetime) -> datetime:
    """
    Clamp a datetime to be within [start, end].
    """
    if dt < start:
        return start
    if dt > end:
        return end
    return dt

def calculate_time_in_period(
    item_created: datetime,
    item_closed: Optional[datetime],
    period_start: datetime,  # Corrected parameter name
    period_end: datetime
) -> Optional[timedelta]:
    """
    Returns the amount of time spent in [period_start, period_end].
    If item_closed is None, treat it as 'still open' and clamp at period_end.
    """
    # Clamp the creation time
    start = clamp_datetime(item_created, period_start, period_end)
    
    # If closed time is missing, treat as open through the end of the period
    closed = item_closed or period_end
    closed = clamp_datetime(closed, period_start, period_end)
    
    # If after clamping, start is still after closed, no time in period
    if start > closed:
        return timedelta(0)
    
    return closed - start

# ----------------------------
# Helper Functions
# ----------------------------

def format_timedelta(td: Optional[timedelta]) -> str:
    if not td:
        return "N/A"
    total_seconds = int(td.total_seconds())
    days, remainder = divmod(total_seconds, 86400)
    hours, remainder = divmod(remainder, 3600)
    minutes, _ = divmod(remainder, 60)
    parts = []
    if days > 0:
        parts.append(f"{days}d")
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    return ' '.join(parts) if parts else "0m"

def average_time(time_list: List[Optional[timedelta]]) -> str:
    if not time_list:
        return "N/A"
    total_seconds = sum(td.total_seconds() for td in time_list if td)
    count = len([td for td in time_list if td])
    if count == 0:
        return "N/A"
    avg_seconds = total_seconds / count
    avg_td = timedelta(seconds=avg_seconds)
    return format_timedelta(avg_td)

def determine_type(labels: List[str]) -> str:
    if 'type_bug' in labels:
        return 'Bug'
    elif 'type_feature' in labels:
        return 'Feature'
    else:
        return 'Other'

def extract_assigned_at(events: List[Dict[str, Any]], username: str) -> Optional[datetime]:
    """
    Extract the most recent assigned date for the user from the PR or Issue events.
    """
    assigned_dates = [
        datetime.fromisoformat(event['created_at'])
        for event in events
        if event['event'] == 'assigned' and event.get('assignee') and event['assignee'].lower() == username.lower()
    ]
    if assigned_dates:
        return max(assigned_dates)  # Most recent assignment
    return None

def calculate_estimated_hours(lines_added: int, lines_deleted: int) -> float:
    """
    Estimate hours based on lines added and deleted.
    """
    return (lines_added + lines_deleted) * HOURS_PER_LINE

# ----------------------------
# Environment and Initialization
# ----------------------------

def get_env_variable(var_name: str) -> str:
    """
    Retrieve environment variable or exit if not found.
    """
    value = os.getenv(var_name)
    if not value:
        logging.critical(f"Environment variable {var_name} not set.")
        sys.exit(1)
    return value

def initialize_redis() -> redis.Redis:
    """
    Initialize and return a Redis client.
    """
    try:
        client = redis.Redis(host='localhost', port=6379, db=0)
        client.ping()
        logging.info("Connected to Redis successfully.")
        return client
    except redis.ConnectionError:
        logging.critical("Could not connect to Redis. Make sure Redis server is running.")
        sys.exit(1)

# ----------------------------
# GitHub Rate Limit Check
# ----------------------------

def check_rate_limit(g: Github):
    """
    Check GitHub API rate limits and sleep if necessary.
    """
    try:
        rate_limit = g.get_rate_limit()
        core_limit = rate_limit.core
        remaining = core_limit.remaining
        reset_timestamp = core_limit.reset.timestamp()
        reset_time = datetime.fromtimestamp(reset_timestamp, timezone.utc)
        logging.info(f"Rate Limit: {remaining}/{core_limit.limit} remaining. Resets at {reset_time.isoformat()}")
        if remaining < 100:  # Adjust threshold as needed
            sleep_seconds = (reset_time - datetime.now(timezone.utc)).total_seconds() + 10  # Adding buffer
            logging.warning(f"Approaching rate limit. Sleeping for {int(sleep_seconds)} seconds until reset.")
            time.sleep(max(sleep_seconds, 0))
    except Exception as e:
        logging.error(f"Error checking rate limit: {e}")

# ----------------------------
# Fetching Functions with Item-Level Caching
# ----------------------------

def fetch_pull_requests(repo: Repository.Repository, username: str, since: datetime, milestone: Optional[str], r: redis.Redis, report_end: datetime) -> List[Dict[str, Any]]:
    """
    Fetch pull requests created by a user, with per-PR caching and clamped time calculations.
    """
    key = cache_key("prs_list", f"{repo.full_name}:{username}:{since.isoformat()}:{milestone}")
    cached = get_cached_data(r, key)
    if cached:
        logging.info(f"Using cached PR list for key: {key}")
        return cached.get('data', [])
    
    prs = []
    try:
        logging.info(f"Fetching pull requests for user '{username}' since {since.isoformat()}")
        fetched_prs = repo.get_pulls(state='all', sort='created', direction='desc', base=repo.default_branch)
        
        for pr in fetched_prs:
            if pr.user and pr.user.login.lower() == username.lower() and pr.created_at >= since:
                if milestone:
                    if pr.milestone and pr.milestone.title == milestone:
                        pass
                    else:
                        continue
                # Build per-PR cache key
                item_key = pr_cache_key(repo.full_name, pr.number)
                
                # Attempt to read from cache
                cached_pr = get_cached_data(r, item_key)
                if cached_pr:
                    pr_data = cached_pr['data']
                    logging.debug(f"Using cached data for PR #{pr.number}")
                else:
                    # Otherwise build the data and store it
                    pr_data = {
                        'id': pr.id,
                        'number': pr.number,
                        'title': pr.title,
                        'state': pr.state,
                        'merged': pr.merged,
                        'url': pr.html_url,
                        'labels': [label.name for label in pr.labels],
                        'created_at': pr.created_at.isoformat(),
                        'closed_at': pr.closed_at.isoformat() if pr.closed_at else None,
                        'merged_at': pr.merged_at.isoformat() if pr.merged_at else None,
                        'milestone': pr.milestone.title if pr.milestone else None
                    }
                    set_cache_data(r, item_key, {'data': pr_data})
                
                prs.append(pr_data)
    
    except Exception as e:
        logging.error(f"Error fetching pull requests: {e}")
    
    # Sort PRs in descending order by creation date
    prs_sorted = sorted(prs, key=lambda x: x['created_at'], reverse=True)
    set_cache_data(r, key, {'data': prs_sorted})
    return prs_sorted

def fetch_pr_commits(repo: Repository.Repository, pr_number: int, r: redis.Redis) -> List[Dict[str, Any]]:
    """
    Fetch commits associated with a PR, with caching and quality assessment.
    """
    key = pr_commits_cache_key(repo.full_name, pr_number)
    cached = get_cached_data(r, key)
    if cached:
        return cached.get('data', [])
    
    commits = []
    try:
        logging.info(f"Fetching commits for PR #{pr_number}")
        pr = repo.get_pull(number=pr_number)
        for commit in pr.get_commits():
            # Evaluate the commit message
            quality_info = evaluate_commit_message_quality(commit.commit.message)
            
            commit_data = {
                'sha': commit.sha,
                'author': commit.author.login if commit.author else 'N/A',
                'message': commit.commit.message.split('\n')[0],  # First line of commit message
                'date': commit.commit.author.date.isoformat(),
                'url': commit.html_url,
                'additions': commit.stats.additions,
                'deletions': commit.stats.deletions,
                'quality': quality_info  # Add the quality assessment
            }
            commits.append(commit_data)
    except Exception as e:
        logging.error(f"Error fetching commits for PR #{pr_number}: {e}")
    
    # Sort commits in ascending order by date
    commits_sorted = sorted(commits, key=lambda x: x['date'])
    set_cache_data(r, key, {'data': commits_sorted})
    return commits_sorted

def fetch_pr_files(repo: Repository.Repository, pr_number: int, r: redis.Redis) -> Dict[str, Any]:
    """
    Fetch files changed in a PR and analyze them for insights, including documentation files.
    """
    key = pr_files_cache_key(repo.full_name, pr_number)
    cached = get_cached_data(r, key)
    if cached:
        return cached.get('data', {})
    
    pr_files_data = {
        'files_changed': 0,
        'additions': 0,
        'deletions': 0,
        'includes_tests': False,
        'test_files': [],
        'test_additions': 0,
        'test_deletions': 0,

        # New fields for doc files
        'includes_docs': False,
        'doc_files': [],
        'doc_additions': 0,
        'doc_deletions': 0,
    }

    try:
        logging.info(f"Fetching files for PR #{pr_number}")
        pr = repo.get_pull(number=pr_number)
        files = pr.get_files()
        pr_files_data['files_changed'] = files.totalCount
        for file in files:
            pr_files_data['additions'] += file.additions
            pr_files_data['deletions'] += file.deletions

            # Existing test detection
            if is_test_file(file.filename):
                pr_files_data['includes_tests'] = True
                pr_files_data['test_files'].append(file.filename)
                pr_files_data['test_additions'] += file.additions
                pr_files_data['test_deletions'] += file.deletions

            # New doc detection
            if is_doc_file(file.filename):
                pr_files_data['includes_docs'] = True
                pr_files_data['doc_files'].append(file.filename)
                pr_files_data['doc_additions'] += file.additions
                pr_files_data['doc_deletions'] += file.deletions

    except Exception as e:
        logging.error(f"Error fetching files for PR #{pr_number}: {e}")

    set_cache_data(r, key, {'data': pr_files_data})
    return pr_files_data

def fetch_pr_comments(repo: Repository.Repository, pr_number: int, username: str, r: redis.Redis) -> Dict[str, int]:
    """
    Fetch and count own and others' comments on a PR.
    """
    key = pr_comments_cache_key(repo.full_name, pr_number)
    cached = get_cached_data(r, key)
    if cached:
        return cached.get('data', {'own': 0, 'others': 0})

    comments = {'own': 0, 'others': 0}
    try:
        logging.info(f"Fetching comments for PR #{pr_number}")
        pr = repo.get_pull(number=pr_number)
        
        # Fetch issue comments (main conversation)
        issue_comments = pr.get_issue_comments()
        for comment in issue_comments:
            if comment.user and comment.user.login.lower() == username.lower():
                comments['own'] += 1
            else:
                comments['others'] += 1
        
        # Fetch review comments (comments on code diffs)
        review_comments = pr.get_review_comments()
        for comment in review_comments:
            if comment.user and comment.user.login.lower() == username.lower():
                comments['own'] += 1
            else:
                comments['others'] += 1

    except Exception as e:
        logging.error(f"Error fetching comments for PR #{pr_number}: {e}")

    set_cache_data(r, key, {'data': comments})
    return comments

def fetch_issues_created(repo: Repository.Repository, username: str, since: datetime, milestone: Optional[str], r: redis.Redis, report_end: datetime) -> List[Dict[str, Any]]:
    """
    Fetch issues created by the user, with per-issue caching and clamped time calculations.
    """
    key = cache_key("issues_created_list", f"{repo.full_name}:{username}:{since.isoformat()}:{milestone}")
    cached = get_cached_data(r, key)
    if cached:
        logging.info(f"Using cached issues created list for key: {key}")
        return cached.get('data', [])
    
    issues_created = []
    try:
        logging.info(f"Fetching issues created by user '{username}' since {since.isoformat()}")
        # Use GitHub search API for more accurate results
        query = f"repo:{repo.full_name} type:issue author:{username} created:>={since.strftime('%Y-%m-%d')}"
        issues = repo._requester.requestJsonAndCheck(
            "GET",
            f"/search/issues",
            parameters={"q": query, "per_page": 100}
        )[1]['items']
        
        for issue in issues:
            if milestone:
                if issue.get('milestone') and issue['milestone']['title'] == milestone:
                    pass
                else:
                    continue
            # Build per-issue cache key
            item_key = issue_cache_key(repo.full_name, issue['number'], "created")
            
            # Attempt to read from cache
            cached_issue = get_cached_data(r, item_key)
            if cached_issue:
                issue_data = cached_issue['data']
                logging.debug(f"Using cached data for Issue #{issue['number']}")
            else:
                # Otherwise build the data and store it
                issue_data = {
                    'id': issue['id'],
                    'number': issue['number'],
                    'title': issue['title'],
                    'state': issue['state'],
                    'url': issue['html_url'],
                    'labels': [label['name'] for label in issue['labels']],
                    'created_at': issue['created_at'],
                    'closed_at': issue['closed_at'],
                    'milestone': issue['milestone']['title'] if issue.get('milestone') else None
                }
                set_cache_data(r, item_key, {'data': issue_data})
            
            issues_created.append(issue_data)
    
    except Exception as e:
        logging.error(f"Error fetching issues created by user '{username}': {e}")
    
    # Sort issues in descending order by creation date
    issues_sorted = sorted(issues_created, key=lambda x: x['created_at'], reverse=True)
    set_cache_data(r, key, {'data': issues_sorted})
    return issues_sorted

def fetch_issues_assigned(repo: Repository.Repository, username: str, since: datetime, milestone: Optional[str], r: redis.Redis, report_end: datetime) -> List[Dict[str, Any]]:
    """
    Fetch issues assigned to the user, with per-issue caching and clamped time calculations.
    """
    key = cache_key("issues_assigned_list", f"{repo.full_name}:{username}:{since.isoformat()}:{milestone}")
    cached = get_cached_data(r, key)
    if cached:
        logging.info(f"Using cached issues assigned list for key: {key}")
        return cached.get('data', [])
    
    issues_assigned = []
    try:
        logging.info(f"Fetching issues assigned to user '{username}' since {since.isoformat()}")
        # Use GitHub search API for more accurate results
        query = f"repo:{repo.full_name} type:issue assignee:{username} created:>={since.strftime('%Y-%m-%d')}"
        issues = repo._requester.requestJsonAndCheck(
            "GET",
            f"/search/issues",
            parameters={"q": query, "per_page": 100}
        )[1]['items']
        
        for issue in issues:
            if milestone:
                if issue.get('milestone') and issue['milestone']['title'] == milestone:
                    pass
                else:
                    continue
            # Build per-issue cache key
            item_key = issue_cache_key(repo.full_name, issue['number'], "assigned")
            
            # Attempt to read from cache
            cached_issue = get_cached_data(r, item_key)
            if cached_issue:
                issue_data = cached_issue['data']
                logging.debug(f"Using cached data for Issue #{issue['number']}")
            else:
                # Otherwise build the data and store it
                issue_data = {
                    'id': issue['id'],
                    'number': issue['number'],
                    'title': issue['title'],
                    'state': issue['state'],
                    'url': issue['html_url'],
                    'labels': [label['name'] for label in issue['labels']],
                    'created_at': issue['created_at'],
                    'closed_at': issue['closed_at'],
                    'milestone': issue['milestone']['title'] if issue.get('milestone') else None
                }
                set_cache_data(r, item_key, {'data': issue_data})
            
            issues_assigned.append(issue_data)
    
    except Exception as e:
        logging.error(f"Error fetching issues assigned to user '{username}': {e}")
    
    # Sort issues in descending order by creation date
    issues_sorted = sorted(issues_assigned, key=lambda x: x['created_at'], reverse=True)
    set_cache_data(r, key, {'data': issues_sorted})
    return issues_sorted

def fetch_issue_events(repo: Repository.Repository, issue_number: int, r: redis.Redis) -> List[Dict[str, Any]]:
    """
    Fetch events for a specific issue.
    """
    key = issue_cache_key(repo.full_name, issue_number, "events")
    cached = get_cached_data(r, key)
    if cached:
        logging.debug(f"Using cached events for Issue #{issue_number}")
        return cached.get('data', [])
    
    events = []
    try:
        issue = repo.get_issue(number=issue_number)
        fetched_events = issue.get_events()
        for event in fetched_events:
            event_data = {
                'event': event.event,
                'actor': event.actor.login if event.actor else None,
                'created_at': event.created_at.isoformat(),
                'assignee': event.assignee.login.lower() if event.assignee else None,
                'label': event.label.name if event.label else None,
                'milestone': event.milestone.title if event.milestone else None,
                'commit_id': event.commit_id if hasattr(event, 'commit_id') else None,
                'action': event.action if hasattr(event, 'action') else None
            }
            events.append(event_data)
    except Exception as e:
        logging.error(f"Error fetching events for Issue #{issue_number}: {e}")
    
    set_cache_data(r, key, {'data': events})
    return events

def fetch_issue_comments(repo: Repository.Repository, issue_number: int, username: str, r: redis.Redis) -> Dict[str, int]:
    """
    Fetch and count own and others' comments on an issue.
    """
    key = issue_comments_cache_key(repo.full_name, issue_number)
    cached = get_cached_data(r, key)
    if cached:
        return cached.get('data', {'own': 0, 'others': 0})
    comments = {'own': 0, 'others': 0}
    try:
        logging.info(f"Fetching comments for Issue #{issue_number}")
        issue = repo.get_issue(number=issue_number)
        fetched_comments = issue.get_comments()
        for comment in fetched_comments:
            if comment.user and comment.user.login.lower() == username.lower():
                comments['own'] += 1
            else:
                comments['others'] += 1
    except Exception as e:
        logging.error(f"Error fetching comments for Issue #{issue_number}: {e}")
    set_cache_data(r, key, {'data': comments})
    return comments

def fetch_issues_details(
    repo: Repository.Repository, 
    issues: List[Dict[str, Any]], 
    username: str, 
    r: redis.Redis, 
    report_end: datetime,
    period_start: datetime  # [FIX] Added parameter
) -> List[Dict[str, Any]]:
    """
    Fetch detailed information for a list of issues (either created or assigned).
    """
    detailed_issues = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Prepare futures for fetching issue events
        issue_event_futures = {executor.submit(fetch_issue_events, repo, issue['number'], r): issue['number'] for issue in issues}
        # Prepare futures for fetching issue comments
        issue_comments_futures = {executor.submit(fetch_issue_comments, repo, issue['number'], username, r): issue['number'] for issue in issues}
        
        # Collect issue events
        issue_events_results = {}
        for future in concurrent.futures.as_completed(issue_event_futures):
            issue_number = issue_event_futures[future]
            try:
                events = future.result()
                issue_events_results[issue_number] = events
            except Exception as e:
                logging.error(f"Error fetching events for Issue #{issue_number}: {e}")
                issue_events_results[issue_number] = []
        
        # Collect issue comments
        issue_comments_results = {}
        for future in concurrent.futures.as_completed(issue_comments_futures):
            issue_number = issue_comments_futures[future]
            try:
                comments = future.result()
                issue_comments_results[issue_number] = comments
            except Exception as e:
                logging.error(f"Error fetching comments for Issue #{issue_number}: {e}")
                issue_comments_results[issue_number] = {'own': 0, 'others': 0}
    
    # Assemble detailed issues
    for issue in issues:
        issue_number = issue['number']
        events = issue_events_results.get(issue_number, [])
        comments = issue_comments_results.get(issue_number, {'own': 0, 'others': 0})
        issue_detail = issue.copy()
        created_at = datetime.fromisoformat(issue_detail['created_at'])
        closed_at = datetime.fromisoformat(issue_detail['closed_at']) if issue_detail['closed_at'] else None
        
        # Extract assigned_at from events
        assigned_at = extract_assigned_at(events, username)
        
        # Calculate time_taken with clamped times
        time_taken = calculate_time_in_period(
            item_created=created_at, 
            item_closed=closed_at, 
            period_start=period_start,  # [FIX] Corrected keyword
            period_end=report_end
        )
        
        issue_detail['events'] = events
        issue_detail['comments'] = comments
        issue_detail['assigned_at'] = assigned_at.isoformat() if assigned_at else 'N/A'
        issue_detail['time_taken'] = time_taken
        detailed_issues.append(issue_detail)
    
    return detailed_issues

def fetch_issues_created_details(
    repo: Repository.Repository, 
    issues_created: List[Dict[str, Any]], 
    username: str, 
    r: redis.Redis, 
    period_start: datetime,  # [FIX] Added parameter
    report_end: datetime
) -> List[Dict[str, Any]]:
    """
    Fetch detailed information for issues created by the user.
    """
    return fetch_issues_details(repo, issues_created, username, r, report_end, period_start)  # [FIX] Passed 'period_start'

def fetch_issues_assigned_details(
    repo: Repository.Repository, 
    issues_assigned: List[Dict[str, Any]], 
    username: str, 
    r: redis.Redis, 
    period_start: datetime,  # [FIX] Added parameter
    report_end: datetime
) -> List[Dict[str, Any]]:
    """
    Fetch detailed information for issues assigned to the user.
    """
    return fetch_issues_details(repo, issues_assigned, username, r, report_end, period_start)  # [FIX] Passed 'period_start'

def fetch_all_pr_details(repo: Repository.Repository, pr_numbers: List[int], username: str, r: redis.Redis) -> Dict[int, Dict[str, Any]]:
    """
    Fetch commits, files, and comments for all PRs concurrently.
    """
    pr_details = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        # Prepare futures for commits, files, and comments
        futures = {
            executor.submit(fetch_pr_commits, repo, pr_number, r): ('commits', pr_number)
            for pr_number in pr_numbers
        }
        futures.update({
            executor.submit(fetch_pr_files, repo, pr_number, r): ('files', pr_number)
            for pr_number in pr_numbers
        })
        futures.update({
            executor.submit(fetch_pr_comments, repo, pr_number, username, r): ('comments', pr_number)
            for pr_number in pr_numbers
        })

        for future in concurrent.futures.as_completed(futures):
            data_type, pr_number = futures[future]
            try:
                data = future.result()
                if pr_number not in pr_details:
                    pr_details[pr_number] = {}
                pr_details[pr_number][data_type] = data
            except Exception as e:
                logging.error(f"Error fetching {data_type} for PR #{pr_number}: {e}")
                if pr_number not in pr_details:
                    pr_details[pr_number] = {}
                pr_details[pr_number][data_type] = {} if data_type != 'comments' else {'own': 0, 'others': 0}
    
    return pr_details

def evaluate_commit_message_quality(message: str) -> Dict[str, Any]:
    """
    Evaluate the quality of a commit message.
    """
    lines = message.strip().splitlines()
    first_line = lines[0] if lines else ""
    length = len(message.strip())
    
    # Basic heuristics
    too_short = length < 20
    multiple_lines = len(lines) > 1
    references_issue = bool(re.search(r'\b(issue|fixes|closes)\s*#\d+', message, re.IGNORECASE))
    
    # Additional heuristics
    proper_format = len(lines) > 1 and lines[1].strip() == ""
    actionable_verb = bool(re.match(r'^(fix|add|update|remove|refactor|improve|resolve|implement|test|style|docs|chore|ci|build)\b', first_line, re.IGNORECASE))
    
    res = {
        'length': length,
        'too_short': too_short,
        'multiple_lines': multiple_lines,
        'references_issue': references_issue,
        'proper_format': proper_format,
        'actionable_verb': actionable_verb,
        'first_line': first_line,
    }
    logging.debug(f"Commit message '{first_line}' quality evaluation: {res}")
    return res

# ----------------------------
# Report Generation
# ----------------------------

def generate_markdown_report(username: str, repo_name: str, metrics: Dict[str, Any], output_file: str):
    logging.info(f"Generating Markdown report: {output_file}")
    with open(output_file, 'w') as f:
        # Header
        generated_on = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        f.write(f"# GitHub Productivity Report for `{username}` in `{repo_name}`\n\n")
        f.write(f"Generated on {generated_on}\n\n")
        f.write("Please note that this report is based on the data retrieved from GitHub's GraphQL API and may not cover all aspects of productivity, but it gives some indication.\n\n")
        f.write("There are some assumptions e.g HOURS_PER_LINE = 0.05  # Estimation: 0.05 hours per line added/deleted\n\n")
        f.write("When evaluated on time spent on task it should be time for planning + time for coding + time for testing\n\n")



        # Commits Summary
        f.write("## Commits Summary\n")
        f.write(f"- **Total Commits**: {metrics['commit_stats']['total_commits']}\n")
        f.write(f"- **Commits Referencing Issues**: {metrics['commit_quality']['references_issue']}\n")
        f.write(f"- **Commits by Others**: {metrics['commit_stats']['commits_by_others']}\n")
        f.write(f"- **Average Commits per PR**: {metrics['commit_stats']['avg_commits_per_pr']}\n")
        f.write(f"- **Total Estimated Hours Based on Code Delivered**: {metrics['commit_stats']['total_estimated_hours']:.2f} hours\n\n")
        
        # Commit Quality Summary
        f.write("## Commit Messages Quality Summary\n")
        total_commits_evaluated = metrics['commit_quality']['references_issue']  # Adjusted for PR-only commits
        f.write(f"- **Total Commits Evaluated**: {total_commits_evaluated} (out of {metrics['commit_stats']['total_commits']})\n")
        f.write(f"- **Commits Referencing Issues**: {metrics['commit_quality']['references_issue']}\n")
        f.write("\n")
        
        # Pull Requests Summary
        f.write("## Pull Requests Summary\n")
        f.write(f"- **Total PRs**: {metrics['prs']['created']}\n")
        f.write(f"- **PRs Merged**: {metrics['prs']['merged']}\n")
        f.write(f"- **Average Time to Merge/Close PRs**: {metrics['prs']['avg_merge_close_time']}\n")
        f.write(f"- **Average Time Spent on PRs**: {metrics['prs']['avg_time_spent']}\n")
        f.write(f"- **Comments on Own PRs**: {metrics['prs']['comments_own']}\n")
        f.write(f"- **Comments on Others' PRs**: {metrics['prs']['comments_others']}\n")
        f.write("\n")
        
        # Pull Requests Summary Table
        f.write("### Pull Requests Summary Table\n")
        if metrics['detailed_prs']:
            f.write("| Number | Title | Type | State | Created At | Time Spent | Commits | Lines Added | Lines Removed | Includes Tests | Includes Docs | Estimated Hours | Comments (Own) | Comments (Others) |\n")
            f.write("|--------|-------|------|-------|------------|------------|---------|-------------|---------------|-----------------|----------------|-----------------|----------------|-------------------|\n")
            for pr in metrics['detailed_prs']:
                pr_type = determine_type(pr['labels'])
                time_taken = format_timedelta(pr.get('time_taken'))
                comments = pr.get('comments', {'own': 0, 'others': 0})
                created_at = pr['created_at']
                pr_files = pr.get('pr_files', {})
                includes_tests = "Yes" if pr_files.get('includes_tests') else "No"
                includes_docs = "Yes" if pr_files.get('includes_docs') else "No"
                commits_count = len(pr.get('commits', []))
                lines_added = pr.get('lines_added', 0)
                lines_removed = pr.get('lines_removed', 0)
                estimated_hours = f"{pr.get('estimated_hours', 0):.2f}"
                # Make the title a clickable link
                title_link = f"[{pr['title']}]({pr['url']})"
                f.write(f"| {pr['number']} | {title_link} | {pr_type} | {pr['state']} | {created_at} | {time_taken} | {commits_count} | {lines_added} | {lines_removed} | {includes_tests} | {includes_docs} | {estimated_hours} | {comments['own']} | {comments['others']} |\n")
            f.write("\n")
        else:
            f.write("No pull requests found for the specified criteria.\n\n")
        
        # Detailed Pull Requests with Commits and Event Timelines
        f.write("## Detailed Pull Requests with Commits and Event Timelines\n")
        for pr in metrics['detailed_prs']:
            pr_type = determine_type(pr['labels'])
            comments = pr.get('comments', {'own': 0, 'others': 0})
            created_at = pr['created_at']
            time_taken = format_timedelta(pr.get('time_taken'))
            pr_files = pr.get('pr_files', {})
            includes_tests = "Yes" if pr_files.get('includes_tests') else "No"
            includes_docs = "Yes" if pr_files.get('includes_docs') else "No"
            commits = pr.get('commits', [])
            commits_count = len(commits)
            lines_added = pr.get('lines_added', 0)
            lines_removed = pr.get('lines_removed', 0)
            estimated_hours = f"{pr.get('estimated_hours', 0):.2f}"
            # Make the title a clickable link
            title_link = f"[{pr['title']}]({pr['url']})"
            f.write(f"#### PR #{pr['number']}: {title_link}\n")
            f.write(f"- **State**: {pr['state']}\n")
            f.write(f"- **Type**: {pr_type}\n")
            f.write(f"- **Merged**: {'Yes' if pr['merged'] else 'No'}\n")
            f.write(f"- **Created At**: {created_at}\n")
            f.write(f"- **Time Taken**: {time_taken}\n")
            f.write(f"- **Commits**: {commits_count}\n")
            f.write(f"- **Lines Added**: {lines_added}\n")
            f.write(f"- **Lines Removed**: {lines_removed}\n")
            f.write(f"- **Includes Tests**: {includes_tests}\n")
            f.write(f"- **Includes Docs**: {includes_docs}\n")
            f.write(f"- **Estimated Hours Based on Code Delivered**: {estimated_hours}\n")
            f.write(f"- **Comments (Own)**: {comments['own']}\n")
            f.write(f"- **Comments (Others)**: {comments['others']}\n\n")
            
            # Commits Table
            if commits:
                f.write("##### Commits in this PR:\n")
                f.write("| SHA | Author | Message | Date | Additions | Deletions | Quality |\n")
                f.write("|-----|--------|---------|------|-----------|-----------|---------|\n")
                for commit in commits:
                    sha_short = commit['sha'][:7]
                    author = commit['author']
                    message = commit['message']
                    date = commit['date']
                    additions = commit['additions']
                    deletions = commit['deletions']
                    
                    # Assess commit quality
                    quality = commit.get('quality', {})
                    quality_assessment = []
                    if quality.get('too_short'):
                        quality_assessment.append("Too Short")
                    if quality.get('multiple_lines'):
                        quality_assessment.append("Multiple Lines")
                    if quality.get('references_issue'):
                        quality_assessment.append("References Issue")
                    quality_str = ", ".join(quality_assessment) if quality_assessment else "Good"
                    
                    link = commit['url']
                    f.write(f"| `{sha_short}` | {author} | {message} | {date} | {additions} | {deletions} | {quality_str} |\n")
                f.write("\n")
            else:
                f.write("##### No commits found for this PR.\n\n")
            
            # Event Timeline
            f.write("##### Event Timeline:\n")
            if pr['events']:
                for event in pr['events']:
                    event_time = event['created_at']
                    actor = event['actor'] or 'N/A'
                    action = event['event']
                    details = []
                    if event.get('label'):
                        details.append(f"Label: {event['label']}")
                    if event.get('milestone'):
                        details.append(f"Milestone: {event['milestone']}")
                    if event.get('assignee'):
                        details.append(f"Assignee: {event['assignee']}")
                    if event.get('commit_id'):
                        details.append(f"Commit: {event['commit_id']}")
                    if event.get('action'):
                        details.append(f"Action: {event['action']}")
                    details_str = ", ".join(details) if details else ""
                    f.write(f"  - **{event_time}**: **{actor}** performed **{action}**{': ' + details_str if details_str else ''}\n")
            else:
                f.write("  - No events found.\n")
            f.write("\n")
        
        # Pull Requests Statistics
        f.write("## Pull Requests Statistics\n")
        f.write(f"- **Total Commits**: {metrics['commit_stats']['total_commits']}\n")
        f.write(f"- **Commits Referencing Issues**: {metrics['commit_stats']['commits_by_user']}\n")
        f.write(f"- **Commits by Others**: {metrics['commit_stats']['commits_by_others']}\n")
        f.write(f"- **Average Commits per PR**: {metrics['commit_stats']['avg_commits_per_pr']}\n")
        f.write(f"- **Total Estimated Hours Based on Code Delivered**: {metrics['commit_stats']['total_estimated_hours']:.2f} hours\n\n")
        
        # Test Metrics Section
        f.write("## Test Metrics\n")
        f.write(f"- **Total Test Files Changed Across All PRs**: {metrics['test_metrics']['total_test_files_changed']}\n")
        f.write(f"- **Total Additions in Test Files**: {metrics['test_metrics']['total_test_additions']}\n")
        f.write(f"- **Total Deletions in Test Files**: {metrics['test_metrics']['total_test_deletions']}\n\n")
        
        # Documentation Metrics Section
        f.write("## Documentation Metrics\n")
        f.write(f"- **Total Documentation Files Changed Across All PRs**: {metrics['doc_metrics']['total_doc_files_changed']}\n")
        f.write(f"- **Total Additions in Documentation Files**: {metrics['doc_metrics']['total_doc_additions']}\n")
        f.write(f"- **Total Deletions in Documentation Files**: {metrics['doc_metrics']['total_doc_deletions']}\n\n")
        
        # Total Hours Spent on PRs
        f.write("## Total Hours Spent on PRs\n")
        f.write(f"- **Total Estimated Hours**: {metrics['total_hours_spent']:.2f} hours\n\n")
        
        # Issues Created Summary
        f.write("## Issues Created Summary\n")
        f.write(f"- **Total Issues Created**: {metrics['issues_created']['total']}\n")
        f.write(f"- **Issues Closed**: {metrics['issues_created']['closed']}\n")
        f.write(f"- **Average Time to Close**: {metrics['issues_created']['avg_close_time']}\n")
        f.write(f"- **Average Time Spent**: {metrics['issues_created']['avg_time_spent']}\n\n")
        
        # Issues Created Detailed Summary
        f.write("### Issues Created Detailed Summary\n")
        f.write("| Number | Title | Type | State | Created At | Assigned At | Time Taken | Comments (Own) | Comments (Others) |\n")
        f.write("|--------|-------|------|-------|------------|-------------|------------|----------------|-------------------|\n")
        for issue in metrics['detailed_issues_created']:
            issue_type = determine_type(issue['labels'])
            time_taken = format_timedelta(issue.get('time_taken'))
            comments = issue.get('comments', {'own': 0, 'others': 0})
            created_at = issue['created_at']
            assigned_at = issue.get('assigned_at', 'N/A')
            # Make the title a clickable link
            title_link = f"[{issue['title']}]({issue['url']})"
            f.write(f"| {issue['number']} | {title_link} | {issue_type} | {issue['state']} | {created_at} | {assigned_at} | {time_taken} | {comments['own']} | {comments['others']} |\n")
        f.write("\n")
        
        # Issues Assigned Summary
        f.write("## Issues Assigned Summary\n")
        f.write(f"- **Total Issues Assigned**: {metrics['issues_assigned']['total']}\n")
        f.write(f"- **Issues Closed**: {metrics['issues_assigned']['closed']}\n")
        f.write(f"- **Average Time to Close**: {metrics['issues_assigned']['avg_close_time']}\n")
        f.write(f"- **Average Time Spent**: {metrics['issues_assigned']['avg_time_spent']}\n\n")
        
        # Issues Assigned Detailed Summary
        f.write("### Issues Assigned Detailed Summary\n")
        f.write("| Number | Title | Type | State | Created At | Assigned At | Time Taken | Comments (Own) | Comments (Others) |\n")
        f.write("|--------|-------|------|-------|------------|-------------|------------|----------------|-------------------|\n")
        for issue in metrics['detailed_issues_assigned']:
            issue_type = determine_type(issue['labels'])
            time_taken = format_timedelta(issue.get('time_taken'))
            comments = issue.get('comments', {'own': 0, 'others': 0})
            created_at = issue['created_at']
            assigned_at = issue.get('assigned_at', 'N/A')
            # Make the title a clickable link
            title_link = f"[{issue['title']}]({issue['url']})"
            f.write(f"| {issue['number']} | {title_link} | {issue_type} | {issue['state']} | {created_at} | {assigned_at} | {time_taken} | {comments['own']} | {comments['others']} |\n")
        f.write("\n")
        
        # Detailed Issues Assigned
        f.write("## Detailed Issues Assigned with Event Timelines\n")
        for issue in metrics['detailed_issues_assigned']:
            issue_type = determine_type(issue['labels'])
            comments = issue.get('comments', {'own': 0, 'others': 0})
            created_at = issue['created_at']
            assigned_at = issue.get('assigned_at', 'N/A')
            # Make the title a clickable link
            title_link = f"[{issue['title']}]({issue['url']})"
            f.write(f"#### Issue #{issue['number']}: {title_link}\n")
            f.write(f"- **State**: {issue['state']}\n")
            f.write(f"- **Type**: {issue_type}\n")
            f.write(f"- **Created At**: {created_at}\n")
            f.write(f"- **Assigned At**: {assigned_at}\n")
            f.write(f"- **Closed At**: {issue['closed_at'] or 'N/A'}\n")
            f.write(f"- **Milestone**: {issue['milestone'] or 'N/A'}\n")
            f.write(f"- **Time Taken**: {format_timedelta(issue.get('time_taken'))}\n")
            f.write(f"- **Comments (Own)**: {comments['own']}\n")
            f.write(f"- **Comments (Others)**: {comments['others']}\n")
            f.write("##### Event Timeline:\n")
            if issue['events']:
                for event in issue['events']:
                    event_time = event['created_at']
                    actor = event['actor'] or 'N/A'
                    action = event['event']
                    details = []
                    if event.get('label'):
                        details.append(f"Label: {event['label']}")
                    if event.get('milestone'):
                        details.append(f"Milestone: {event['milestone']}")
                    if event.get('assignee'):
                        details.append(f"Assignee: {event['assignee']}")
                    if event.get('commit_id'):
                        details.append(f"Commit: {event['commit_id']}")
                    if event.get('action'):
                        details.append(f"Action: {event['action']}")
                    details_str = ", ".join(details) if details else ""
                    f.write(f"  - **{event_time}**: **{actor}** performed **{action}**{': ' + details_str if details_str else ''}\n")
            else:
                f.write("  - No events found.\n")
            f.write("\n")
        
        # Detailed Issues Created
        f.write("## Detailed Issues Created with Event Timelines\n")
        for issue in metrics['detailed_issues_created']:
            issue_type = determine_type(issue['labels'])
            comments = issue.get('comments', {'own': 0, 'others': 0})
            created_at = issue['created_at']
            assigned_at = issue.get('assigned_at', 'N/A')
            # Make the title a clickable link
            title_link = f"[{issue['title']}]({issue['url']})"
            f.write(f"#### Issue #{issue['number']}: {title_link}\n")
            f.write(f"- **State**: {issue['state']}\n")
            f.write(f"- **Type**: {issue_type}\n")
            f.write(f"- **Created At**: {created_at}\n")
            f.write(f"- **Assigned At**: {assigned_at}\n")
            f.write(f"- **Closed At**: {issue['closed_at'] or 'N/A'}\n")
            f.write(f"- **Milestone**: {issue['milestone'] or 'N/A'}\n")
            f.write(f"- **Time Taken**: {format_timedelta(issue.get('time_taken'))}\n")
            f.write(f"- **Comments (Own)**: {comments['own']}\n")
            f.write(f"- **Comments (Others)**: {comments['others']}\n")
            f.write("##### Event Timeline:\n")
            if issue['events']:
                for event in issue['events']:
                    event_time = event['created_at']
                    actor = event['actor'] or 'N/A'
                    action = event['event']
                    details = []
                    if event.get('label'):
                        details.append(f"Label: {event['label']}")
                    if event.get('milestone'):
                        details.append(f"Milestone: {event['milestone']}")
                    if event.get('assignee'):
                        details.append(f"Assignee: {event['assignee']}")
                    if event.get('commit_id'):
                        details.append(f"Commit: {event['commit_id']}")
                    if event.get('action'):
                        details.append(f"Action: {event['action']}")
                    details_str = ", ".join(details) if details else ""
                    f.write(f"  - **{event_time}**: **{actor}** performed **{action}**{': ' + details_str if details_str else ''}\n")
            else:
                f.write("  - No events found.\n")
            f.write("\n")
        
        # Pull Requests Statistics
        f.write("## Pull Requests Statistics\n")
        f.write(f"- **Total Commits**: {metrics['commit_stats']['total_commits']}\n")
        f.write(f"- **Commits Referencing Issues**: {metrics['commit_stats']['commits_by_user']}\n")
        f.write(f"- **Commits by Others**: {metrics['commit_stats']['commits_by_others']}\n")
        f.write(f"- **Average Commits per PR**: {metrics['commit_stats']['avg_commits_per_pr']}\n")
        f.write(f"- **Total Estimated Hours Based on Code Delivered**: {metrics['commit_stats']['total_estimated_hours']:.2f} hours\n\n")
        
        # Test Metrics Section
        f.write("## Test Metrics\n")
        f.write(f"- **Total Test Files Changed Across All PRs**: {metrics['test_metrics']['total_test_files_changed']}\n")
        f.write(f"- **Total Additions in Test Files**: {metrics['test_metrics']['total_test_additions']}\n")
        f.write(f"- **Total Deletions in Test Files**: {metrics['test_metrics']['total_test_deletions']}\n\n")
        
        # Documentation Metrics Section
        f.write("## Documentation Metrics\n")
        f.write(f"- **Total Documentation Files Changed Across All PRs**: {metrics['doc_metrics']['total_doc_files_changed']}\n")
        f.write(f"- **Total Additions in Documentation Files**: {metrics['doc_metrics']['total_doc_additions']}\n")
        f.write(f"- **Total Deletions in Documentation Files**: {metrics['doc_metrics']['total_doc_deletions']}\n\n")
        
        # Total Hours Spent on PRs
        f.write("## Total Hours Spent on PRs\n")
        f.write(f"- **Total Estimated Hours**: {metrics['total_hours_spent']:.2f} hours\n\n")

# ----------------------------
# Helper Functions for File Classification
# ----------------------------

def is_test_file(filename: str) -> bool:
    """
    Determine if a file is a test file based on its name or path.
    """
    test_patterns = [
        r'/tests?/.*',        # /test/ or /tests/ directories
        r'.*_test\..*$',       # files ending with _test.*
        r'.*\bTest\b.*\.py$',  # files containing 'Test' in the name for Python
    ]
    return any(re.match(pattern, filename, re.IGNORECASE) for pattern in test_patterns)

def is_doc_file(filename: str) -> bool:
    """
    Determine if a file is a documentation file based on its extension or name.
    """
    doc_extensions = ['.md', '.rst', '.txt']
    doc_keywords = ['README', 'CHANGELOG', 'CONTRIBUTING', 'LICENSE']
    if any(filename.lower().endswith(ext) for ext in doc_extensions):
        return True
    if any(keyword.lower() in filename.lower() for keyword in doc_keywords):
        return True
    return False

# ----------------------------
# Main Execution Function
# ----------------------------

def main():
    # Argument Parsing
    parser = argparse.ArgumentParser(description="Generate a comprehensive GitHub productivity report for a user within a repository.")
    parser.add_argument('user', help='GitHub username')
    parser.add_argument('repo', help='Repository name in the format owner/repo-name')
    parser.add_argument('-t', '--time', default=DEFAULT_TIME_FRAME, help='Time constraint (e.g., "30d" for 30 days, "3m" for 3 months). Default is "7d".')
    parser.add_argument('-m', '--milestone', help='Milestone to filter by')
    parser.add_argument('--flush', action='store_true', help='Clear the Redis cache')
    
    args = parser.parse_args()
    
    # Initialize Redis
    r = initialize_redis()
    
    if args.flush:
        flush_cache(r)
    
    # Parse time frame
    try:
        since = parse_time_frame(args.time)
    except ValueError as ve:
        logging.critical(f"Error parsing time frame: {ve}")
        print(f"Error parsing time frame: {ve}")
        sys.exit(1)
    
    report_end = datetime.now(timezone.utc)
    
    # GitHub Authentication
    github_token = get_env_variable('GITHUB_TOKEN')
    g = Github(github_token, per_page=100)
    
    # Get Repository
    try:
        repo = g.get_repo(args.repo)
        logging.info(f"Accessed repository '{args.repo}' successfully.")
    except Exception as e:
        logging.critical(f"Error accessing repository {args.repo}: {e}")
        print(f"Error accessing repository {args.repo}: {e}")
        sys.exit(1)
    
    username = args.user.lower()
    
    # Check initial rate limit
    check_rate_limit(g)
    
    # Data Gathering
    logging.info("Starting data gathering...")
    print("Fetching pull requests...")
    prs = fetch_pull_requests(repo, username, since, args.milestone, r, report_end)
    
    if not prs:
        logging.info("No pull requests found for the specified criteria.")
        print("No pull requests found for the specified criteria.")
    
    # Fetch all PR details concurrently
    print("Fetching pull request details (commits, files, comments)...")
    pr_numbers = [pr['number'] for pr in prs]
    pr_details_dict = fetch_all_pr_details(repo, pr_numbers, username, r)
    
    # Assemble detailed PRs with all fetched data
    print("Assembling detailed PRs...")
    detailed_prs = []
    for pr in prs:
        pr_number = pr['number']
        details = pr_details_dict.get(pr_number, {})
        commits = details.get('commits', [])
        pr_files = details.get('files', {})
        comments = details.get('comments', {'own': 0, 'others': 0})
        
        created_at = datetime.fromisoformat(pr['created_at'])
        closed_at = datetime.fromisoformat(pr['closed_at']) if pr['closed_at'] else None
        merged_at = datetime.fromisoformat(pr['merged_at']) if pr['merged_at'] else None
        
        # Determine end_time
        if merged_at:
            end_time = merged_at
        elif closed_at:
            end_time = closed_at
        else:
            end_time = report_end
        
        # Calculate time_taken with clamped times
        time_taken = calculate_time_in_period(
            item_created=created_at,
            item_closed=merged_at if merged_at else closed_at,
            period_start=since,
            period_end=report_end
        )
        
        # Calculate total lines added and removed in the PR
        total_lines_added = pr_files.get('additions', 0)
        total_lines_removed = pr_files.get('deletions', 0)
        
        # Calculate estimated hours based on code delivered
        estimated_hours = calculate_estimated_hours(total_lines_added, total_lines_removed)
        
        pr_detail = pr.copy()
        pr_detail['events'] = []  # Events can be fetched and added if needed
        pr_detail['comments'] = comments  # Updated with fetched comments
        pr_detail['time_taken'] = time_taken
        pr_detail['pr_files'] = pr_files  # Include PR files data
        pr_detail['commits'] = commits  # Include commits data
        pr_detail['lines_added'] = total_lines_added
        pr_detail['lines_removed'] = total_lines_removed
        pr_detail['estimated_hours'] = estimated_hours
        detailed_prs.append(pr_detail)
    
    # Fetch Issues Created
    print("Fetching issues created by user...")
    issues_created = fetch_issues_created(repo, username, since, args.milestone, r, report_end)
    
    # Fetch Issues Assigned
    print("Fetching issues assigned to user...")
    issues_assigned = fetch_issues_assigned(repo, username, since, args.milestone, r, report_end)
    
    # Fetch detailed issues created
    print("Fetching detailed issues created...")
    detailed_issues_created = fetch_issues_created_details(repo, issues_created, username, r, since, report_end)
    
    # Fetch detailed issues assigned
    print("Fetching detailed issues assigned...")
    detailed_issues_assigned = fetch_issues_assigned_details(repo, issues_assigned, username, r, since, report_end)
    
    # Metrics Calculation
    metrics = {}
    
    # Pull Requests
    merged_prs = [pr for pr in prs if pr['merged']]
    merge_close_times = [
        calculate_time_in_period(
            item_created=datetime.fromisoformat(pr['created_at']),
            item_closed=datetime.fromisoformat(pr['merged_at']) if pr['merged_at'] else None,
            period_start=since,
            period_end=report_end
        )
        for pr in prs
        if pr['merged_at']
    ]
    close_times = [
        calculate_time_in_period(
            item_created=datetime.fromisoformat(pr['created_at']),
            item_closed=datetime.fromisoformat(pr['closed_at']) if pr['closed_at'] else None,
            period_start=since,
            period_end=report_end
        )
        for pr in prs
        if pr['closed_at'] and not pr['merged']
    ]
    time_to_merge_close = merge_close_times + close_times
    time_spent_prs = [
        pr['time_taken'] for pr in detailed_prs
        if pr['time_taken']
    ]
    avg_merge_close_time = average_time(time_to_merge_close)
    avg_time_spent_prs = average_time(time_spent_prs)
    
    # Count comments on PRs
    comments_own = sum(pr['comments']['own'] for pr in detailed_prs)
    comments_others = sum(pr['comments']['others'] for pr in detailed_prs)
    
    # Track state changes
    state_changes = {}
    for pr in prs:
        state = pr['state']
        state_changes[state] = state_changes.get(state, 0) + 1
    
    # Aggregate Commit Statistics
    total_commits = sum(len(pr['commits']) for pr in detailed_prs)
    commits_referencing_issues = sum(
        1 for pr in detailed_prs for commit in pr['commits'] 
        if commit.get('quality', {}).get('references_issue', False)
    )
    commits_by_user = commits_referencing_issues  # Assuming these are authored by the user
    commits_by_others = total_commits - commits_by_user
    avg_commits_per_pr = len(prs) and (sum(len(pr['commits']) for pr in detailed_prs) / len(prs)) or 0.0
    total_estimated_hours = sum(pr.get('estimated_hours', 0) for pr in detailed_prs)
    
    metrics['prs'] = {
        'created': len(prs),
        'merged': len(merged_prs),
        'avg_merge_close_time': avg_merge_close_time,
        'avg_time_spent': avg_time_spent_prs,
        'comments_own': comments_own,
        'comments_others': comments_others,
        'state_changes': state_changes
    }
    metrics['detailed_prs'] = detailed_prs  # Include detailed PRs with commits and time estimates
    
    metrics['commit_quality'] = {
        'references_issue': commits_referencing_issues,
    }
    
    metrics['commit_stats'] = {
        'total_commits': total_commits,
        'commits_by_user': commits_by_user,
        'commits_by_others': commits_by_others,
        'avg_commits_per_pr': f"{avg_commits_per_pr:.2f}",
        'total_estimated_hours': total_estimated_hours
    }
    
    # Issues Created
    closed_issues_created = [issue for issue in issues_created if issue['state'] == 'closed']
    time_to_close_created = [
        calculate_time_in_period(
            item_created=datetime.fromisoformat(issue['created_at']),
            item_closed=datetime.fromisoformat(issue['closed_at']) if issue['closed_at'] else None,
            period_start=since,
            period_end=report_end
        )
        for issue in issues_created
        if issue['closed_at']
    ]
    time_spent_created = [
        issue.get('time_taken') for issue in detailed_issues_created
        if issue.get('time_taken') is not None
    ]
    
    avg_close_time_created = average_time(time_to_close_created)
    avg_time_spent_created = average_time(time_spent_created)
    
    metrics['issues_created'] = {
        'total': len(issues_created),
        'closed': len(closed_issues_created),
        'avg_close_time': avg_close_time_created,
        'avg_time_spent': avg_time_spent_created
    }
    metrics['detailed_issues_created'] = detailed_issues_created  # Include detailed issues created with events and comments
    
    # Issues Assigned
    closed_issues_assigned = [issue for issue in issues_assigned if issue['state'] == 'closed']
    time_to_close_assigned = [
        calculate_time_in_period(
            item_created=datetime.fromisoformat(issue['created_at']),
            item_closed=datetime.fromisoformat(issue['closed_at']) if issue['closed_at'] else None,
            period_start=since,
            period_end=report_end
        )
        for issue in detailed_issues_assigned
        if issue.get('time_taken') is not None
    ]
    time_spent_assigned = time_to_close_assigned  # Same as time_to_close_assigned
    
    avg_close_time_assigned = average_time(time_to_close_assigned)
    avg_time_spent_assigned = average_time(time_spent_assigned)
    
    metrics['issues_assigned'] = {
        'total': len(issues_assigned),
        'closed': len(closed_issues_assigned),
        'avg_close_time': avg_close_time_assigned,
        'avg_time_spent': avg_time_spent_assigned
    }
    metrics['detailed_issues_assigned'] = detailed_issues_assigned  # Include detailed issues assigned with events and comments
    
    # Test Metrics
    total_test_files_changed = sum(len(pr['pr_files'].get('test_files', [])) for pr in detailed_prs)
    total_test_additions = sum(pr['pr_files'].get('test_additions', 0) for pr in detailed_prs)
    total_test_deletions = sum(pr['pr_files'].get('test_deletions', 0) for pr in detailed_prs)
    
    # Documentation Metrics
    total_doc_files_changed = sum(len(pr['pr_files'].get('doc_files', [])) for pr in detailed_prs)
    total_doc_additions = sum(pr['pr_files'].get('doc_additions', 0) for pr in detailed_prs)
    total_doc_deletions = sum(pr['pr_files'].get('doc_deletions', 0) for pr in detailed_prs)
    
    metrics['test_metrics'] = {
        'total_test_files_changed': total_test_files_changed,
        'total_test_additions': total_test_additions,
        'total_test_deletions': total_test_deletions
    }
    
    metrics['doc_metrics'] = {
        'total_doc_files_changed': total_doc_files_changed,
        'total_doc_additions': total_doc_additions,
        'total_doc_deletions': total_doc_deletions
    }
    
    metrics['total_hours_spent'] = total_estimated_hours
    
    # Generate Report
    repo_safe = args.repo.replace('/', '-')
    output_file = f"{username}.{repo_safe}.md"
    generate_markdown_report(username, args.repo, metrics, output_file)
    logging.info(f"Report generation completed: {output_file}")
    print(f"Report generation completed: {output_file}")

# ----------------------------
# Run the Script
# ----------------------------

if __name__ == "__main__":
    main()
