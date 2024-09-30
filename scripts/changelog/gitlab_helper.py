import gitlab
import logging
import requests
from config import myscale_repo_config, NO_CHGLOG_LABEL

logger = logging.getLogger(__name__)

class GitLab:
    """GitLab API wrapper."""
    
    def __init__(self, private_token) -> None:
        self._session = requests.Session()
        self._session.trust_env = False
        self.gl = gitlab.Gitlab(
            myscale_repo_config["url"],
            private_token=private_token,
            api_version=4,
            session=self._session,
        )
        self.project = self.gl.projects.get(myscale_repo_config["project_id"])
        self._retries = 0
    
    def get_merged_requests(self, *args, **kwargs) -> list:
        """Get merged requests from GitLab."""
        res = []
        mrs = self.project.mergerequests.list(*args, **kwargs)
        update_after = kwargs.get("updated_after")
        for mr in mrs:
            if mr.state == "merged":
                merge_time = mr.merged_at
                if merge_time < update_after:
                    logger.debug("MR %s was merged before the update_after date %s", mr, update_after)
                    continue
                res.append(mr)
        return res
    
    def create_mr(self, source_branch, target_branch, title, description) -> None:
        """Create a merge request."""
        self.project.mergerequests.create(
            {
                "source_branch": source_branch,
                "target_branch": target_branch,
                "title": title,
                "description": description,
                "labels": [NO_CHGLOG_LABEL],
            }
        )
    
    @property
    def retries(self) -> int:
        return self._retries
    
    @retries.setter
    def retries(self, value: int) -> None:
        self._retries = value
    
    