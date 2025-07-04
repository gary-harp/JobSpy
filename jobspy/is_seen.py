from abc import ABC
from typing import Set

from .can_skip_job_post import CanSkipJobPost


class IsSeen(CanSkipJobPost):

    _seen_jobs : Set[str]

    def __init__(self):
        self.seen_jobs = set()

    def can_skip(self, job_id: str) -> bool:
        return job_id in self.seen_jobs

    def add_seen(self, job_id: str):
        self._seen_jobs.add(job_id)

