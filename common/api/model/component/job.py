from pydantic import Field
from pydantic.dataclasses import dataclass
from typing import List

from ..vault_model import VaultModel

@dataclass
class Link(VaultModel):
    rel: str = None
    href: str = None
    method: str = None
    accept: str = None


@dataclass
class Job(VaultModel):
    created_by: int = None
    created_date: str = None
    id: int = None
    method: str = None
    run_end_date: str = None
    run_start_date: str = None
    status: str = None
    title: str = None
    links: List[Link] = Field(default_factory=list)
