from marshmallow import Schema, fields


class SelectSchema(Schema):
    label = fields.String(required=True)
    value = fields.String(required=True)


class DomainSelectSchema(Schema):
    value = fields.Integer(required=True)
    label_pt = fields.String(required=True)
    label_en = fields.String(required=True)


class ErrorSchema(Schema):
    error = fields.String(required=True)
    detail = fields.String(required=False, allow_none=True)
