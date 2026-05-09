# noqa: E501
import os

from flask import request
from flask.views import MethodView
from flask_smorest import Blueprint

from app.utils.bilingual import bilingual_response
from app.utils.send_email import send_email

contact_bp = Blueprint(
    "contact",
    "contact",
    url_prefix="/contact",
)

GMAIL_TO = os.getenv("GMAIL_TO", "alenz@uneb.br")


@contact_bp.route("")
class Contact(MethodView):
    @contact_bp.response(200)
    @contact_bp.alt_response(400, description="Payload inválido")
    @contact_bp.alt_response(500, description="Falha ao enviar contato")
    def post(self):
        data = request.get_json(silent=True) or {}
        name = (data.get("name") or "").strip()
        email = (data.get("email") or "").strip()
        message = (data.get("message") or "").strip()
        subject = (data.get("subject") or "").strip()
        to = (data.get("to") or "").strip()

        if not name or not email or not message or not subject:
            return bilingual_response(400, "Preencha todos os campos", "Please fill in all fields")

        try:
            send_email(
                subject="[BEM] Contato",
                content=f"""
                    <html>
                        <body style="font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, 'Open Sans', 'Helvetica Neue', sans-serif; line-height:1.6;">
                            <h2>Nova mensagem de contato</h2>
                            <p><strong>Nome:</strong> {name}</p>
                            <p><strong>Assunto:</strong> {subject}</p>
                            <p><strong>Email:</strong> {email}</p>
                            <hr style="margin:20px 0;">
                            <p><strong>Mensagem:</strong></p>
                            <p>{message}</p>
                        </body>
                    </html>
                """,  # noqa: E501
                to=to or GMAIL_TO,
            )
            return {"ok": True}
        except Exception:
            return bilingual_response(500, "Falha ao enviar email", "Failed to send email")
