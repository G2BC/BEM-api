from marshmallow import Schema, fields


class ObservationSchema(Schema):
    id = fields.Integer(dump_only=True)
    source = fields.String(dump_only=True)
    external_id = fields.String(dump_only=True)
    latitude = fields.Float(dump_only=True)
    longitude = fields.Float(dump_only=True)
    location_obscured = fields.Boolean(dump_only=True)
    observed_on = fields.Date(dump_only=True)
    quality_grade = fields.String(allow_none=True, dump_only=True)
    photo_url = fields.String(allow_none=True, dump_only=True)
    url = fields.String(allow_none=True, dump_only=True)


class ObservationListSchema(Schema):
    observations = fields.List(fields.Nested(ObservationSchema), dump_only=True)
    total = fields.Integer(dump_only=True)
