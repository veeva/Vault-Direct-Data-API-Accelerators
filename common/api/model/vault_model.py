"""
Module used to define an abstract class that is extended by VaultResponse and
other classes that represent nested objects within an API response.
"""

from abc import ABC
from typing import Any, List, Dict

from pydantic.dataclasses import dataclass
from pydantic.fields import Field


@dataclass
class VaultModel(ABC):
    """
      Abstract class that is extended by classes that represent nested objects
      within an API response.

      Attributes:
          vault_model_data (Dict[str, Any]): A dictionary to store the data associated with the Vault object.
              The keys are field names, and the values are the corresponding data
          field_names (List[str]): A list of field names for the Vault object
    """

    vault_model_data: Dict[str, Any] = Field(default_factory=dict)
    field_names: List[str] = Field(default_factory=list)

    def __post_init__(self):
        # Sets the field_names and vault_model_data attributes when the class is instantiated.

        for key, value in self.__dict__.items():
            if key == 'vault_model_data':
                continue
            if key == 'field_names':
                continue
            if key not in self.field_names:
                if value is not None:
                    self.field_names.append(key)
            if key not in self.vault_model_data:
                if value is not None:
                    self.vault_model_data[key] = value
