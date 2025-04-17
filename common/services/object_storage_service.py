from abc import ABC, abstractmethod


class ObjectStorageService(ABC):
    def __init__(self):
        self.credentials: dict | None = None

    @abstractmethod
    def retrieve_credentials(self, step: str) -> dict:
        pass
