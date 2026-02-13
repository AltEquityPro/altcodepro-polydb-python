from polydb.utils import setup_logger


from abc import ABC, abstractmethod
from typing import Any, Dict, List


class QueueAdapter(ABC):
    """Base class for Queue/Message services"""

    def __init__(self):
        self.logger = setup_logger(self.__class__.__name__)

    @abstractmethod
    def send(self, message: Dict[str, Any], queue_name: str = "default") -> str:
        """Send message to queue"""
        pass

    @abstractmethod
    def receive(self, queue_name: str = "default", max_messages: int = 1) -> List[Dict[str, Any]]:
        """Receive messages from queue"""
        pass

    @abstractmethod
    def delete(self, message_id: str, queue_name: str = "default") -> bool:
        """Delete message from queue"""
        pass