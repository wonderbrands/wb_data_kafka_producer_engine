from abc import ABC, abstractmethod
from typing import Any

class MessageBroker(ABC):
    @abstractmethod
    def authenticate(self, host, port, user, password, database_name, any_other_credential=None) -> Any:
        pass

    @abstractmethod
    def consume(self, database) -> None:    
        
    
        