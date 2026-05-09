import os

import requests

GMAIL_USER = os.getenv("GMAIL_USER", "")
APP_ENV = os.getenv("APP_ENV", "development")


def send_email(subject: str, content: str, to: str) -> None:
    if APP_ENV == "development":
        return

    res = requests.post(
        os.getenv("MAKE_EMAIL_WEBHOOK_URL", ""),
        json={
            "subject": subject,
            "content": content,
            "to": to,
        },
        headers={"x-make-apikey": os.getenv("MAKE_EMAIL_WEBHOOK_KEY", "")},
    )

    if res.status_code != 200:
        raise Exception("Falha ao enviar email. Tente novamente mais tarde")
