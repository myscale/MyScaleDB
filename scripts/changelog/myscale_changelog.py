#!/usr/bin/env python3
import argparse
import logging
import os
import re
import subprocess
import tempfile
import sys

from datetime import datetime, date, timedelta
from subprocess import CalledProcessError, DEVNULL
from typing import List, Optional, Tuple, Dict

from fuzzywuzzy.fuzz import ratio
from gitlab_helper import GitLab
from git_helper import release_branch, git_runner as runner
from config import REPO_PATH, MYSCALE_VERSION_FILE, CHANGELOG_PATH, NO_CHGLOG_LABEL, MYSCALE_DEFAULT_BRANCH

FROM_REF = ""
TO_REF = ""
SHA_IN_CHANGELOG = []
MR_IN_CHANGELOG = []
SHA_BLACKLIST = []
BRANCH_NEED_PUSH = []
TAG_NEED_PUSH = []

VERSIONS_TEMPLATE = """# This variables autochanged by scripts/myscale_version_helper.py:

# NOTE: has nothing common with DBMS_TCP_PROTOCOL_VERSION,
# only DBMS_TCP_PROTOCOL_VERSION should be incremented on protocol changes.
SET(MYSCALE_VERSION_MAJOR {major})
SET(MYSCALE_VERSION_MINOR {minor})
SET(MYSCALE_VERSION_PATCH {patch})
SET(MYSCALE_VERSION_DESCRIBE {describe})
SET(MYSCALE_VERSION_STRING {string})
# end of autochange

"""

categories_preferred_order = (
    "Backward Incompatible Change",
    "New Feature",
    "Performance Improvement",
    "Improvement",
    "Bug Fix",
    "Build/Testing/Packaging Improvement",
    "Other",
)

myscale_label_to_category = {
    "enhancement": "Features & Improvements",
    "feature": "Features & Improvements",
    "bug-fix": "Fixs",
    "defualt": "Features & Improvements",
}

# gl = GitLab()
logger = logging.getLogger(__name__)

class Description:
    def __init__(
        self, number: int, user: Dict, html_url: str, entry: str, category: str
    ):
        self.number = number
        self.html_url = html_url
        self.user = user
        self.entry = entry
        self.category = category

    @property
    def formatted_entry(self) -> str:
        # Substitute issue links.
        # 1) issue number w/o markdown link
        entry = re.sub(
            r"([^[])!([0-9]{4,})",
            r"\1[!\2](https://git.moqi.ai/mqdb/ClickHouse/-/issues/\2)",
            self.entry,
        )
        # 2) issue URL w/o markdown link
        entry = re.sub(
            r"([^(])https://git.moqi.ai/mqdb/ClickHouse/-/issues/([0-9]{4,})",
            r"\1[!\2](https://git.moqi.ai/mqdb/ClickHouse/-/issues/\2)",
            entry,
        )
        # It's possible that we face a secondary rate limit.
        # In this case we should sleep until we get it
        user_name = self.user["name"]
        user_url = self.user["web_url"]
        return (
            f"- {entry}\n [!{self.number}]({self.html_url}) "
            f"([{user_name}]({user_url}))."
        )

    # Sort PR descriptions by numbers
    def __eq__(self, other) -> bool:
        if not isinstance(self, type(other)):
            return NotImplemented
        return self.number == other.number

    def __lt__(self, other: "Description") -> bool:
        return self.number < other.number


def remove_prefix(text: str, prefix: str) -> str:
    """Remove prefix from a string."""
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def edit_comments(old_comments: List[str]) -> List[str]:
    """Edit comments with vim."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as fd:
        fd.writelines(old_comments)
        fd.flush()
        temp_file_name = fd.name

    subprocess.call(["vim", temp_file_name])

    with open(temp_file_name, "r") as fd:
        new_comments = fd.readlines()
        logging.debug("Old comments: %s, new comments: %s", old_comments, new_comments)

    subprocess.call(["rm", temp_file_name])

    return new_comments


def generate_description(mrs: object) -> Description:
    """Generate description for a PR."""
    descriptions = {}
    for mr in mrs:
        mr_dict = mr.asdict()
        logging.debug("Generating description for MR: %s", mr_dict)
        number = mr_dict["iid"]
        user = mr_dict["author"]
        html_url = mr_dict["web_url"]
        entry = mr_dict["title"]
        labels = mr_dict["labels"]
        category = myscale_label_to_category["defualt"]
        if NO_CHGLOG_LABEL in labels:
            continue
        for label in labels:
            if label in myscale_label_to_category.keys():
                category = myscale_label_to_category[label]
                break
        desc = Description(number, user, html_url, entry, category)
        if (
            mr_dict["merge_commit_sha"] in SHA_BLACKLIST
            or str(number) not in MR_IN_CHANGELOG
            or mr_dict["merge_commit_sha"] not in SHA_IN_CHANGELOG
        ):
            continue
        if desc is not None:
            if desc.category not in descriptions:
                descriptions[desc.category] = []
            descriptions[desc.category].append(desc)
    for descs in descriptions.values():
        descs.sort()

    return descriptions


def parse_args() -> argparse.Namespace:
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate a changelog based on merged MRs."
    )
    parser.add_argument(
        "-d",
        "--debug",
        action="store_true",
        help="Enable debug logging.",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="count",
        default=0,
        help="set the script verbosity, could be used multiple.",
    )
    parser.add_argument(
        "-b",
        "--branch",
        type=str,
        help="Branch to release the changelog for.",
    )
    parser.add_argument(
        "--version",
        type=str,
        help="Version to release the changelog for. such as 1.2.3 or 1.2.3-rc1",
    )
    parser.add_argument(
        "--from-ref",
        help="The commit SHA or branch name to start the changelog from.",
    )
    parser.add_argument(
        "--private-token",
        type=str,
        help="GitLab access private token.",
    )
    return parser.parse_args()


def get_release_version(str) -> str:
    """Get release version from the branch name."""
    return re.search(r"v[0-9]+\.[0-9]+\.[0-9]+(-rc\d+)?$", str).group(0)


def check_tag(tag: str) -> None:
    """Check if the tag is valid."""
    if not re.match(r"^myscale-v[0-9]+\.[0-9]+\.[0-9]+(-rc\d+)?$", tag):
        raise argparse.ArgumentTypeError(
            "tag should be as myscale-v1.2.3 or myscale-v1.2.3-rc1"
        )

    logging.info("Fetch all tags")
    runner.run("git fetch --tags -f", stderr=DEVNULL)
    logging.info("Check if the tag exists")
    if runner.run(f"git tag --list {tag}") != "":
        raise argparse.ArgumentTypeError(f"tag {tag} already exists")
    logging.info(f"Create tag: {tag}")
    version = get_release_version(tag)
    runner.run(f"git tag {tag} -m 'Myscale Release {version}'", stderr=DEVNULL)


def update_tag(tag: str) -> None:
    """Update the tag."""
    logging.info("Update the tag")
    version = get_release_version(tag)
    runner.run(f"git tag -d {tag}", stderr=DEVNULL)
    runner.run(f"git tag {tag} -m 'Myscale Release {version}'", stderr=DEVNULL)


def check_branch(base_branch: Optional[str], tag: str) -> str:
    """Check if the branch is valid."""
    if not re.match(r"^myscale-v[0-9]+\.[0-9]+\.[0-9]+(-rc\d+)?$", tag):
        raise argparse.ArgumentTypeError(
            "Tag should be as release/v1.2.3 or release/v1.2.3-rc1"
        )
    version = get_release_version(tag)
    new_branch = f"release/{version}"
    logging.info("Fetch all branches")
    runner.run("git fetch origin", stderr=DEVNULL)
    logging.info("Delete the branch if it exists")
    try:
        runner.run(f"git branch -D {new_branch}", stderr=DEVNULL)
    except:
        pass
    if base_branch is not None:
        runner.run(f"git branch {new_branch} origin/{base_branch}", stderr=DEVNULL)
    else:
        runner.run(f"git branch {new_branch}", stderr=DEVNULL)
    runner.run(f"git checkout {new_branch}", stderr=DEVNULL)
    return new_branch
    # runner.run(f"git checkout {branch}", stderr=DEVNULL)
    # runner.run(f"git pull origin {branch} -f", stderr=DEVNULL)


def check_ref(from_ref: Optional[str], to_ref: str) -> None:
    global FROM_REF, TO_REF
    TO_REF = to_ref

    runner.run(f"git rev-parse {TO_REF}")
    if from_ref is None:
        # get last tag
        cmd = f"git tag --list 'myscale-v*' | grep -v '{TO_REF}' | sort -V | tail -n 1"
        FROM_REF = runner.run(cmd)
    else:
        runner.run(f"git rev-parse {from_ref}")
        FROM_REF = from_ref


def set_sha_in_changelog() -> None:
    global SHA_IN_CHANGELOG
    SHA_IN_CHANGELOG = runner.run(
        f"git log --format=format:%H {FROM_REF}..{TO_REF}"
    ).split("\n")


def set_mr_in_changelog():
    global MR_IN_CHANGELOG
    MR_IN_CHANGELOG = runner.run(
        f"git log {FROM_REF}..{TO_REF} --grep='See merge request' --format='%b' | grep 'See merge request' | sed 's/.*!//'"
    ).split("\n")


def write_changelog(descriptions: List[Description], output: str) -> str:
    """Write changelog to a file."""
    year = datetime.now().year
    # to_commit = runner.run(f"git rev-parse {TO_REF}^{{}}")[:11]
    # from_commit = runner.run(f"git rev-parse {FROM_REF}^{{}}")[:11]
    db_version = get_release_version(TO_REF)
    with open(output, "w") as fd:
        fd.write(
            f"# MyScale Release Notes\n\n"
            f"## {year}\n\n"
            f"### [{db_version}](https://git.moqi.ai/mqdb/ClickHouse/-/tags/{TO_REF}) - {datetime.now().strftime('%Y-%m-%d')}\n\n"
        )

        seen_categories = []
        categories = list(set(myscale_label_to_category.values()))
        for category in categories:
            if category in descriptions:
                seen_categories.append(category)
                fd.write(f"{category}\n\n")
                for desc in descriptions[category]:
                    fd.write(f"{desc.formatted_entry}\n")
                fd.write("\n\n")

        for category in sorted(descriptions):
            if category not in seen_categories:
                fd.write(f"{category}\n\n")
                for desc in descriptions[category]:
                    fd.write(f"{desc.formatted_entry}\n\n")
                fd.write("\n\n")
    # edit_comments(output)
    subprocess.call(["vim", output])
    return output


def insert_description_to_changelog(
    descriptions: List[Description], output: str, check_mr_deep=40
) -> str:
    """Insert description to the changelog file."""
    with open(output, "r", encoding="utf-8") as fd:
        changelog_lines = fd.readlines()

    seen_mr = set()
    for i, line in enumerate(changelog_lines):
        if re.search(r" \[![0-9]+\]\(", line):
            mr_number = int(re.search(r"!([0-9]+)", line).group(1))
            seen_mr.add(int(mr_number))
        if i > check_mr_deep:
            break
    insert_index = None
    logging.info(f"Seen MRs: {seen_mr}")

    for i, line in enumerate(changelog_lines):
        if re.search(r"^## [0-9]+$", line):
            insert_index = i + 1
            break

    if insert_index is None:
        raise argparse.ArgumentTypeError("Changelog not found")

    has_new_comments = False
    new_comments = []
    if insert_index is not None:
        db_version = get_release_version(TO_REF)
        new_comments.append(
            f"\n### [{db_version}](https://git.moqi.ai/mqdb/ClickHouse/-/tags/{TO_REF}) - {datetime.now().strftime('%Y-%m-%d')}\n\n"
        )
        seen_categories = []
        categories = list(set(myscale_label_to_category.values()))
        for category in categories:
            if category in descriptions:
                seen_categories.append(category)
                new_comments.append(f"{category}\n\n")
                for desc in descriptions[category]:
                    if desc.number in seen_mr:
                        logger.warning(
                            f"MR {desc.number} already in changelog, will be skipped"
                        )
                        continue
                    has_new_comments = True
                    new_comments.append(f"{desc.formatted_entry}\n")
                new_comments.append("\n")

        for category in sorted(descriptions):
            if category not in seen_categories:
                new_comments.append(f"{category}\n\n")
                for desc in descriptions[category]:
                    if desc.number in seen_mr:
                        logger.warning(
                            f"MR {desc.number} already in changelog, will be skipped"
                        )
                        continue
                    has_new_comments = True
                    new_comments.append(f"{desc.formatted_entry}\n\n")
                new_comments.append("\n")

    if not has_new_comments:
        raise argparse.ArgumentTypeError("No new comments to insert")
    final_comments = edit_comments(new_comments)
    changelog_lines[insert_index:insert_index] = final_comments
    with open(output, "w", encoding="utf-8") as fd:
        fd.writelines(changelog_lines)
    return output


def update_myscale_version_file(tag: str) -> str:
    version = get_release_version(tag)
    version = re.search(r"([0-9]+\.[0-9]+\.[0-9]+)", version).group(1)
    major, minor, patch = version.split(".")
    with open(MYSCALE_VERSION_FILE, "w", encoding="utf-8") as fd:
        fd.write(
            VERSIONS_TEMPLATE.format(
                major=major,
                minor=minor,
                patch=patch,
                describe=tag,
                string=remove_prefix(tag, "myscale-v"),
            )
        )
    return MYSCALE_VERSION_FILE


def commit_changes(change_file_list, commit_description: str) -> str:
    assert len(change_file_list) > 0, "No files to commit"
    runner.run(f"git add {' '.join(change_file_list)}")
    runner.run(f"git commit -m '{commit_description}'")
    current_head = runner.run("git rev-parse HEAD")
    return current_head


def update_default_branch_chglog(chg_update_sha: str, tag: str) -> str:
    default_branch = "mqdb-dev"
    version = get_release_version(tag)
    update_branch = f"update-changelog-{version}"
    global BRANCH_NEED_PUSH
    BRANCH_NEED_PUSH.append(update_branch)
    release_branch = f"release/{version}"
    logging.info("Create branch %s", update_branch)
    runner.run(f"git checkout -b {update_branch} origin/{default_branch}")
    chg_update_sha = runner.run(f"git rev-parse {chg_update_sha}")
    runner.run(f"git checkout {release_branch} {CHANGELOG_PATH}")
    runner.run(f"git add {CHANGELOG_PATH}")
    runner.run(f"git commit -m 'Update changelog for {version}'")
    return update_branch


def verify_options(description: str, skip_verify: bool = False) -> None:
    """Ask the user for Y/N confirmation and block the terminal until a valid input is provided."""
    while not skip_verify:
        user_input = input(f"{description} (Y/N): ").strip().upper()
        if user_input == "Y":
            print("You selected Yes.")
            break
        elif user_input == "N":
            print("You selected No.")
            sys.exit(1)
        else:
            print("Invalid input. Please enter 'Y' for Yes or 'N' for No.")


def push_changes() -> None:
    logging.info("Push changes to the remote")
    global BRANCH_NEED_PUSH, TAG_NEED_PUSH
    tag = TAG_NEED_PUSH[0]
    version = get_release_version(tag)
    for b in BRANCH_NEED_PUSH:
        verify_options(f"Push changes to the remote branch {b}?")
        logging.info("Push changes to the remote branch %s", b)
        runner.run(f"git push -u origin {b}")

    verify_options(f"Push tag {tag} to the remote?")
    logging.info("Push tag %s to the remote", tag)
    runner.run(f"git push origin {tag}")


def main():
    log_levels = [logging.WARN, logging.INFO, logging.DEBUG]
    args = parse_args()
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:\n%(message)s",
        level=log_levels[min(args.verbose, 2)],
    )
    if args.debug:
        logging.getLogger("git_helper").setLevel(logging.DEBUG)
        logging.getLogger("gitlab_helper").setLevel(logging.DEBUG)

    tag = f"myscale-v{args.version}"
    base_branch = args.branch
    new_branch = check_branch(base_branch, tag)
    check_tag(tag)
    global BRANCH_NEED_PUSH, TAG_NEED_PUSH
    BRANCH_NEED_PUSH.append(new_branch)
    TAG_NEED_PUSH.append(tag)

    check_ref(args.from_ref, tag)
    set_sha_in_changelog()
    set_mr_in_changelog()

    logging.info("Using the following refs: %s..%s", FROM_REF, TO_REF)

    global SHA_BLACKLIST
    base_commit = runner.run(f"git merge-base '{FROM_REF}^{{}}' '{TO_REF}^{{}}'")
    SHA_BLACKLIST.append(base_commit)
    logging.info("Base commit: %s", base_commit)
    from_data = runner.run(f"git log -1 --format=format:%cs '{base_commit}'")
    to_data = runner.run(f"git log -1 --format=format:%cs '{TO_REF}^{{}}'")
    query_params = {
        "updated_after": (date.fromisoformat(from_data) - timedelta(1)).isoformat(),
        "updated_before": (date.fromisoformat(to_data) + timedelta(1)).isoformat(),
        "state": "merged",
    }
    logging.info("Querying MRs with the following params: %s", query_params)
    # global gl
    gl = GitLab(args.private_token)
    mrs = gl.get_merged_requests(**query_params)
    descriptions = generate_description(mrs)
    change_file_list = []
    if not os.path.exists(CHANGELOG_PATH):
        change_file_list.append(write_changelog(descriptions, CHANGELOG_PATH))
    else:
        change_file_list.append(
            insert_description_to_changelog(descriptions, CHANGELOG_PATH)
        )
    change_file_list.append(update_myscale_version_file(tag))
    update_sha = commit_changes(change_file_list, f"bump version to v{args.version}")
    update_tag(tag)
    mr_source_branch = update_default_branch_chglog(update_sha, tag)
    push_changes()
    verify_options(f"Create MR for the branch {mr_source_branch} to {MYSCALE_DEFAULT_BRANCH}?")
    gl.create_mr(
        mr_source_branch,
        MYSCALE_DEFAULT_BRANCH,
        f"Update changelog for {tag}",
        f"Update changelog for {tag}",
    )


if __name__ == "__main__":
    main()
