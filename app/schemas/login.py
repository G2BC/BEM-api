import re

from marshmallow import Schema, ValidationError, fields, validate, validates


class LoginRequestSchema(Schema):
    email = fields.Email(
        required=True,
        error_messages={
            "required": "O campo email é obrigatório",
            "invalid": "Email inválido",
            "null": "O campo email não pode ser nulo",
        },
    )
    password = fields.String(
        required=True,
        load_only=True,
        error_messages={
            "required": "O campo senha é obrigatório",
            "null": "O campo senha não pode ser nulo",
        },
    )


class TokenSchema(Schema):
    access_token = fields.String(required=True, dump_only=True)
    refresh_token = fields.String(dump_only=True)
    must_change_password = fields.Boolean(dump_only=True)


class ChangePasswordSchema(Schema):
    current_password = fields.String(
        required=True,
        load_only=True,
        error_messages={
            "required": "O campo senha atual é obrigatório",
            "null": "O campo senha atual não pode ser nulo",
        },
    )
    new_password = fields.String(
        required=True,
        load_only=True,
        validate=validate.Length(min=8, max=128, error="A senha deve ter entre 8 e 128 caracteres"),
        error_messages={
            "required": "O campo nova senha é obrigatório",
            "null": "O campo nova senha não pode ser nulo",
        },
    )

    @validates("new_password")
    def validate_password_strength(self, value: str):
        if not re.search(r"[A-Z]", value):
            raise ValidationError("A senha precisa ter ao menos uma letra maiúscula")
        if not re.search(r"[a-z]", value):
            raise ValidationError("A senha precisa ter ao menos uma letra minúscula")
        if not re.search(r"\d", value):
            raise ValidationError("A senha precisa ter ao menos um número")


class AdminResetPasswordSchema(Schema):
    user_id = fields.String(required=True, dump_only=True)
    temporary_password = fields.String(required=True, dump_only=True)
    must_change_password = fields.Boolean(required=True, dump_only=True)
