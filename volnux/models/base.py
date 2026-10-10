from formax import BaseModel, InitStrategy

from volnux.mixins import KeyValueStoreIntegrationMixin
from volnux.backends.fields import DateTimeField, DTConfig


class GovernanceModel(KeyValueStoreIntegrationMixin, BaseModel):
    """Base model for all governance entities.

    Provides:
        - Self-persistence via KeyValueStoreIntegrationMixin
        - Automatic ForeignKey backreference registration
        - Standard timestamp fields
        - OrJSON serialization support
    """

    created_at: DateTimeField[DTConfig(auto_now_add=True)]
    updated_at: DateTimeField[DTConfig(auto_now=True)]

    class Config:
        init_strategy = InitStrategy.DATACLASS
        unsafe_hash = False
        frozen = False
        eq = True

    def __hash__(self) -> int:
        return hash(self.id)
