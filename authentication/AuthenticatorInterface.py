from abc import ABC, abstractmethod

class AuthenticatorInterface(ABC):
    
    @abstractmethod
    def connect(self) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def import_config(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def authenticate(self) -> None:
        raise NotImplementedError
