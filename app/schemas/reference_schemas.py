from marshmallow import Schema, fields, validate


class ReferenceSchema(Schema):
    id = fields.Integer(dump_only=True)
    doi = fields.String(allow_none=True)
    url = fields.String(allow_none=True)
    apa = fields.String(allow_none=True)


class ReferenceAssociateExistingSchema(Schema):
    """Associate an already-existing reference to a species."""

    reference_id = fields.Integer(required=True, strict=True, validate=validate.Range(min=1))


class ReferenceCreateAndAssociateSchema(Schema):
    """Create a new reference and associate it to a species in a single step."""

    apa = fields.String(required=True, validate=validate.Length(min=1))
    doi = fields.String(load_default=None, allow_none=True)
    url = fields.String(load_default=None, allow_none=True)


class ReferenceUpdateSchema(Schema):
    """Partial update of an existing reference's fields."""

    apa = fields.String(required=True, validate=validate.Length(min=1))
    doi = fields.String(load_default=None, allow_none=True)
    url = fields.String(load_default=None, allow_none=True)
