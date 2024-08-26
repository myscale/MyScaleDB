#!/usr/bin/env python
"""Helper for GitHub API requests"""
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from os import path as p
from time import sleep
from typing import List, Optional, Tuple, Union

import github

# explicit reimport
# pylint: disable=useless-import-alias
from github.AuthenticatedUser import AuthenticatedUser
from github.GithubException import (
    RateLimitExceededException as RateLimitExceededException,
)
from github.Issue import Issue as Issue
from github.NamedUser import NamedUser as NamedUser
from github.PullRequest import PullRequest as PullRequest
from github.Repository import Repository as Repository

logger = logging.getLogger(__name__)

class GitHub(github.Github):
    def __init__(self, user_or_token, password):
        super().__init__(login_or_token=user_or_token, password=password)
        
    def create_pr(self, repo_name, title, body, head, base):
        repo = self.get_repo(repo_name)
        return repo.create_pull(title=title, body=body, head=head, base=base)
    
    def check_pr_exists(self, repo_name, head, base):
        repo = self.get_repo(repo_name)
        prs = repo.get_pulls(state="open", head=head, base=base)
        # [FYI] use prs.totalCount to get the number of PRs is not correct, it is a bug in PyGithub?
        try:
            pr = prs[0]
            return True
        except IndexError:
            return False

