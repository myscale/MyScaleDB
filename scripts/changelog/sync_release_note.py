#!/usr/bin/env python3
import argparse
import logging
import os
import re
import sys
import subprocess

from config import MYSCALE_DEFAULT_BRANCH, myscale_deploy_guide_repo_config, CHANGELOG_PATH
from git_helper import Runner
from github_helper import GitHub
from myscale_changelog import verify_options

runner = Runner()
NEAREST_TAGS = []
NEWEST_CHANGELOG = []
DEPLOY_REPO_PR_BRANCH = ""
SKIP_VERiFY = False

def parser_args():
    parser = argparse.ArgumentParser(description="Sync release notes from GitLab to myscale deploy guide")
    parser.add_argument(
        "--github-user-or-token",
        required=True,
        help="GitHub user or token",
    )
    parser.add_argument(
        "--github-password",
        default=None,
        type=str,
        help="GitHub password",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase verbosity",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip verify options use default yes",
    )
    return parser.parse_args()

def fetch_deploy_guide_default_branch() -> None:
    if os.path.exists(myscale_deploy_guide_repo_config["repo_name"]):
        runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} fetch origin {myscale_deploy_guide_repo_config['default_branch']}")
    else:
        runner.run(f"git clone {myscale_deploy_guide_repo_config['repo_ssh_url']} --branch {myscale_deploy_guide_repo_config['default_branch']} --single-branch --depth 1")
        runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} rev-parse --abbrev-ref HEAD")
    logging.info(f"Create update release note branch")
    global DEPLOY_REPO_PR_BRANCH
    DEPLOY_REPO_PR_BRANCH = f"update-{NEAREST_TAGS[-1]}-release-note"
    try:
        logging.info(f"Delete branch {DEPLOY_REPO_PR_BRANCH}")
        runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} checkout {myscale_deploy_guide_repo_config['default_branch']}")
        runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} branch -D {DEPLOY_REPO_PR_BRANCH}")
        runner.run("git -C {myscale_deploy_guide_repo_config['repo_name']} stash")
    except subprocess.CalledProcessError:
        logging.warning(f"Branch {DEPLOY_REPO_PR_BRANCH} not found")
    runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} branch {DEPLOY_REPO_PR_BRANCH} {myscale_deploy_guide_repo_config['default_branch']}")
    runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} checkout {DEPLOY_REPO_PR_BRANCH}")

def create_mr(gh: GitHub, source_branch: str, target_branch: str, title: str, description: str) -> None:
    logging.info(f"Create MR from {source_branch} to {target_branch}, in repo {myscale_deploy_guide_repo_config['repo_git_path']}")
    gh.create_pr(myscale_deploy_guide_repo_config["repo_git_path"], title, description, f"{source_branch}", target_branch)

def version_is_valid(version: str) -> bool:
    return re.search(r"^myscale-v[0-9]+\.[0-9]+\.[0-9]+(-rc\d+)?$", version) is not None

def get_version_from_tag(tag: str) -> str:
    if not re.search(r"^myscale-v[0-9]+\.[0-9]+\.[0-9]+(-rc\d+)?$", tag):
        raise ValueError(f"Tag {tag} is not valid")
    return re.search(r"[0-9]+\.[0-9]+\.[0-9]+", tag).group(0)

def get_nearest_tag_list(deep: int = 2) -> list:
    runner.run(f"git fetch --tags")
    if runner.run("git tag --list 'myscale-*'") == "":
        raise ValueError("No tags found")
    tags = runner.run(f"git tag --list 'myscale-*'").split("\n")
    valid_tags = [tag for tag in tags if version_is_valid(tag)]
    sorted_tags = sorted(valid_tags, key=lambda x: tuple(map(int, get_version_from_tag(x).split("."))))
    return sorted_tags[-deep:]

def get_version_from_tag_or_chglog(tag: str) -> str:
    if not re.search(r".*v[0-9]+\.[0-9]+\.[0-9]+(-rc\d+)?.*", tag):
        raise ValueError(f"Tag {tag} is not valid")
    return re.search(r"[0-9]+\.[0-9]+\.[0-9]+(rc\d+)?", tag).group(0)

def get_newest_changelog() -> list:
    is_expected_changelog = False
    newest_changelog = []
    with open(CHANGELOG_PATH, "r") as f:
        for line in f.readlines():
            if is_expected_changelog and re.search("^### \[v[0-9]+\.[0-9]+\.[0-9]+\].*$", line):
                break
            elif not is_expected_changelog and re.search("^### \[v[0-9]+\.[0-9]+\.[0-9]+\].*$", line):
                is_expected_changelog = True
                newest_changelog.append(line)
            elif is_expected_changelog:
                newest_changelog.append(line)
            else:
                continue
    return newest_changelog

def get_has_version_str_from_changelog(changelog: list) -> str:
    for line in changelog:
        logging.debug(f"changelog line: {line}")
        if re.search("^### \[v[0-9]+\.[0-9]+\.[0-9]+\].*$", line):
            return line
    raise ValueError("Changelog has no version string")

def insert_release_note_to_deploy_guide():
    os.chdir(myscale_deploy_guide_repo_config["repo_name"])
    with open(f"{myscale_deploy_guide_repo_config['release_note_file']}", "r", encoding="utf-8") as f:
        changelog_lines = f.readlines()
        
    last_release_version_str = get_has_version_str_from_changelog(changelog_lines[:10])
    last_release_version = get_version_from_tag_or_chglog(last_release_version_str)
    last_tag_version = get_version_from_tag(NEAREST_TAGS[0])
    assert last_release_version == last_tag_version, f"Last release version {last_release_version} and nearest tag {last_tag_version} are not equal"
    
    insert_index = None
    if not insert_index:
        for i, line in enumerate(changelog_lines):
            if re.search(r"^## [0-9]+$", line):
                insert_index = i + 1
                break
    
    global NEWEST_CHANGELOG
    if NEWEST_CHANGELOG[0] != "\n":
        NEWEST_CHANGELOG.insert(0, "\n")
    while NEWEST_CHANGELOG[-1] == "\n":
        NEWEST_CHANGELOG.pop()

    for i, line in enumerate(NEWEST_CHANGELOG):
        sub_line = re.sub(r'\s*\(\[.*?\]\(.*?\)\).*', '', line)
        NEWEST_CHANGELOG[i] = sub_line
    
    changelog_lines[insert_index:insert_index] = NEWEST_CHANGELOG
    with open(f"{myscale_deploy_guide_repo_config['release_note_file']}", "w", encoding="utf-8") as f:
        f.writelines(changelog_lines)

def commit_and_push_to_deploy_guide():
    runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} add {myscale_deploy_guide_repo_config['release_note_file']}")
    runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} commit -m 'Update release note for {NEAREST_TAGS[1]}'")
    verify_options(f"Push changes to {DEPLOY_REPO_PR_BRANCH}? (y/n): ", SKIP_VERiFY)
    runner.run(f"git -C {myscale_deploy_guide_repo_config['repo_name']} push origin {DEPLOY_REPO_PR_BRANCH} --force")

def main():
    args = parser_args()
    if args.yes:
        global SKIP_VERiFY
        SKIP_VERiFY = True
    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:\n%(message)s",
        level=log_levels[min(args.verbose, 2)],
    )
    global NEWEST_CHANGELOG, NEAREST_TAGS
    NEAREST_TAGS = get_nearest_tag_list()
    NEWEST_CHANGELOG = get_newest_changelog()
    tag_version = get_version_from_tag_or_chglog(NEAREST_TAGS[-1])
    has_version_str = get_has_version_str_from_changelog(NEWEST_CHANGELOG)
    changelog_version = get_version_from_tag_or_chglog(has_version_str)
    assert tag_version == changelog_version, f"Tag version {tag_version} and changelog version {changelog_version} are not equal"

    fetch_deploy_guide_default_branch()
    insert_release_note_to_deploy_guide()
    commit_and_push_to_deploy_guide()
    global DEPLOY_REPO_PR_BRANCH
    DEPLOY_REPO_PR_BRANCH = f"update-{NEAREST_TAGS[-1]}-release-note"
    gh = GitHub(args.github_user_or_token, args.github_password)
    if gh.check_pr_exists(myscale_deploy_guide_repo_config["repo_git_path"], DEPLOY_REPO_PR_BRANCH, myscale_deploy_guide_repo_config["default_branch"]):
        logging.info(f"MR from {DEPLOY_REPO_PR_BRANCH} to {myscale_deploy_guide_repo_config['default_branch']} already exists")
    else:
        logging.info(f"Create MR from {DEPLOY_REPO_PR_BRANCH} to {myscale_deploy_guide_repo_config['default_branch']}")
        create_mr(gh, DEPLOY_REPO_PR_BRANCH, myscale_deploy_guide_repo_config["default_branch"], f"Update release note for {NEAREST_TAGS[1]}", "Update release note for the latest release")

if __name__ == "__main__":
    main()


