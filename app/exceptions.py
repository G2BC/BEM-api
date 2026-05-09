"""
Structured bilingual exceptions.

Each exception carries its own PT + EN messages, eliminating the need for a
central translation dictionary.

Usage in services:
    raise AppError(pt="Espécie não encontrada.", en="Species not found.", status=404)
    raise AppPermissionError(pt="Conta inativa.", en="Inactive account.")
    raise AppRuntimeError(pt="Falha no NCBI.", en="NCBI failure.")
"""


class AppError(ValueError):
    """Validation / business-rule error with explicit PT + EN messages."""

    def __init__(self, pt: str, en: str, status: int = 400) -> None:
        super().__init__(pt)
        self.pt = pt
        self.en = en
        self.status = status


class AppPermissionError(PermissionError):
    """Permission / authorization error with explicit PT + EN messages."""

    def __init__(self, pt: str, en: str) -> None:
        super().__init__(pt)
        self.pt = pt
        self.en = en


class AppRuntimeError(RuntimeError):
    """External-service / infrastructure error with explicit PT + EN messages."""

    def __init__(self, pt: str, en: str) -> None:
        super().__init__(pt)
        self.pt = pt
        self.en = en
