from abc import ABC, abstractmethod


class CanSkipJobPost(ABC):

    @abstractmethod
    def can_skip(self, job_id: str) -> bool:
        pass