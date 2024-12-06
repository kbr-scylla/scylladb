#!/usr/bin/env python3

import os
import subprocess
import sys
import re
import tempfile
from typing import Optional, Dict, Any
import requests

GITHUB_API_URL = "https://api.github.com"

def run_git_command(args: list[str]) -> str:
    try:
        result = subprocess.run(["git"] + args, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"Git command failed: {' '.join(args)}\n{e.stderr.strip()}", file=sys.stderr)
        sys.exit(1)

def get_current_branch() -> str:
    return run_git_command(["rev-parse", "--abbrev-ref", "HEAD"])

def get_remote_tracking_branch() -> Optional[str]:
    try:
        return run_git_command(["rev-parse", "--symbolic-full-name", "@{u}"])
    except subprocess.CalledProcessError:
        return None

def fetch_pr_data(repo: str, branch: str, token: str) -> Optional[Dict[str, Any]]:
    headers = {"Authorization": f"token {token}"}
    query = f"head:{branch}"
    response = requests.get(f"{GITHUB_API_URL}/repos/{repo}/pulls", headers=headers, params={"state": "open", "head": query})
    response.raise_for_status()
    prs = response.json()
    return prs[0] if prs else None

def create_new_branch(old_branch: str) -> str:
    match = re.search(r"branch-v(\d+)$", old_branch)
    if not match:
        print(f"Old branch name {old_branch} does not follow the pattern branch-vN.", file=sys.stderr)
        sys.exit(1)

    new_version = int(match.group(1)) + 1
    new_branch = f"branch-v{new_version}"
    run_git_command(["branch", new_branch])
    run_git_command(["checkout", new_branch])
    return new_branch

def edit_description(old_description: str) -> str:
    with tempfile.NamedTemporaryFile(suffix=".md", mode="w+", delete=False) as tmp_file:
        tmp_file.write(old_description + "\n\n# Add your changes below this line\n")
        tmp_file.flush()
        subprocess.run([os.getenv("EDITOR", "vim"), tmp_file.name])
        tmp_file.seek(0)
        return tmp_file.read()

def create_pr(repo: str, base: str, head: str, title: str, body: str, token: str) -> Dict[str, Any]:
    headers = {"Authorization": f"token {token}"}
    payload = {
        "title": title,
        "head": head,
        "base": base,
        "body": body
    }
    response = requests.post(f"{GITHUB_API_URL}/repos/{repo}/pulls", headers=headers, json=payload)
    response.raise_for_status()
    return response.json()

def close_pr(repo: str, pr_number: int, token: str) -> None:
    headers = {"Authorization": f"token {token}"}
    payload = {"state": "closed"}
    response = requests.patch(f"{GITHUB_API_URL}/repos/{repo}/pulls/{pr_number}", headers=headers, json=payload)
    response.raise_for_status()

def main():
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        print("Environment variable GITHUB_TOKEN must be set.", file=sys.stderr)
        sys.exit(1)

    repo = os.getenv("GITHUB_REPO")
    if not repo:
        print("Environment variable GITHUB_REPO must be set (e.g., 'owner/repo').", file=sys.stderr)
        sys.exit(1)

    current_branch = get_current_branch()
    remote_branch = get_remote_tracking_branch()
    if not remote_branch:
        print("Current branch is not tracking any remote branch.", file=sys.stderr)
        sys.exit(1)

    pr_data = fetch_pr_data(repo, remote_branch, token)
    if not pr_data:
        print(f"No open pull request found for branch {remote_branch}.", file=sys.stderr)
        sys.exit(1)

    old_pr_number = pr_data["number"]
    old_pr_description = pr_data["body"]
    base_branch = pr_data["base"]["ref"]
    old_pr_title = pr_data["title"]
    old_pr_url = pr_data["html_url"]

    print(f"Found PR #{old_pr_number}: {old_pr_title}")

    new_branch = create_new_branch(current_branch)
    run_git_command(["push", "-u", "origin", new_branch])

    new_description = edit_description(old_pr_description)
    new_description += f"\n\n_This PR supersedes #{old_pr_number}._"

    new_pr = create_pr(repo, base_branch, new_branch, f"{old_pr_title} (v{new_branch})", new_description, token)
    close_pr(repo, old_pr_number, token)

    print(f"New PR created: {new_pr['html_url']}")
    print(f"Old PR #{old_pr_number} closed.")

if __name__ == "__main__":
    main()
