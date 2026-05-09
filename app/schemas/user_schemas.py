import re

from marshmallow import EXCLUDE, Schema, ValidationError, fields, pre_load, validate, validates

from app.models.user import User
from app.utils.pagination import make_pagination_schema


class UserSchema(Schema):
    id = fields.String(dump_only=True)
    name = fields.String(required=True)
    institution = fields.String(allow_none=True)
    email = fields.Email(
        required=True,
        error_messages={
            "required": "O campo email é obrigatório",
            "invalid": "Email inválido",
            "null": "O campo email não pode ser nulo",
        },
    )
    is_admin = fields.Boolean(dump_only=True)
    role = fields.String(dump_only=True)
    is_curator = fields.Boolean(dump_only=True)
    is_active = fields.Boolean(dump_only=True)
    must_change_password = fields.Boolean(dump_only=True)
    created_at = fields.DateTime(dump_only=True)


class UserCreateSchema(Schema):
    name = fields.String(
        required=True,
        validate=validate.Length(min=2, max=100, error="Nome deve ter entre 2 e 100 caracteres"),
        error_messages={
            "required": "O campo nome é obrigatório",
            "null": "O campo nome não pode ser nulo",
        },
    )
    institution = fields.String(
        allow_none=True,
        validate=validate.Length(max=150, error="Instituição deve ter no máximo 150 caracteres"),
    )
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
        validate=validate.Length(min=8, max=128, error="A senha deve ter entre 8 e 128 caracteres"),
        error_messages={
            "required": "O campo senha é obrigatório",
            "null": "O campo senha não pode ser nulo",
        },
    )

    @validates("password")
    def validate_password_strength(self, value: str):
        if not re.search(r"[A-Z]", value):
            raise ValidationError("A senha precisa ter ao menos uma letra maiúscula")
        if not re.search(r"[a-z]", value):
            raise ValidationError("A senha precisa ter ao menos uma letra minúscula")
        if not re.search(r"\d", value):
            raise ValidationError("A senha precisa ter ao menos um número")


UserPaginationSchema = make_pagination_schema(UserSchema)


class UserRoleUpdateSchema(Schema):
    role = fields.String(
        required=True,
        validate=validate.OneOf(
            User.ROLES,
            error=f"`role` deve ser um de: {', '.join(User.ROLES)}",
        ),
        error_messages={
            "required": "O campo role é obrigatório",
            "null": "O campo role não pode ser nulo",
        },
    )


class UserListQuerySchema(Schema):
    class Meta:
        unknown = EXCLUDE

    search = fields.String(
        allow_none=True,
        validate=validate.Length(max=150, error="`search` deve ter no máximo 150 caracteres"),
    )
    page = fields.Integer(
        allow_none=True,
        validate=validate.Range(min=1, error="`page` deve ser um inteiro >= 1"),
    )
    per_page = fields.Integer(
        allow_none=True,
        validate=validate.Range(min=1, max=100, error="`per_page` deve estar entre 1 e 100"),
    )
    is_active = fields.Boolean(allow_none=True, missing=None)
    isactive = fields.Boolean(allow_none=True, load_only=True, missing=None)

    @pre_load
    def map_isactive_alias(self, data, **kwargs):
        payload = dict(data or {})
        if payload.get("is_active") in (None, "") and "isactive" in payload:
            payload["is_active"] = payload.get("isactive")
        return payload

class UserUpdateSchema(Schema):
    name = fields.String(
        validate=validate.Length(min=2, max=100),
        error_messages={"invalid": "Nome inválido"}
    )
    institution = fields.String(
        allow_none=True,
        validate=validate.Length(max=150)
    )
    email = fields.Email(
        error_messages={"invalid": "Email inválido"}
    )
    
    current_password = fields.String(load_only=True)
    new_password = fields.String(
        load_only=True,
        validate=validate.Length(min=8, max=128)
    )
    confirm_password = fields.String(load_only=True)

    @validates("new_password")
    def validate_new_password_strength(self, value: str):
        if value:
            if not re.search(r"[A-Z]", value):
                raise ValidationError("A nova senha precisa ter ao menos uma letra maiúscula")
            if not re.search(r"[a-z]", value):
                raise ValidationError("A nova senha precisa ter ao menos uma letra minúscula")
            if not re.search(r"\d", value):
                raise ValidationError("A nova senha precisa ter ao menos um número")