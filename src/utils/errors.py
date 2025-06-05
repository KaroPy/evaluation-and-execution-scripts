"""
Modulte to handle errors
"""


class InnkeeprError(Exception):
    """
    Class for Innkeepr Errors
    """

    def __init__(self, http_status, message) -> None:
        self.message = message
        self.http_status = http_status

    def __str__(self) -> str:
        return f"{self.http_status}: {self.message}"
