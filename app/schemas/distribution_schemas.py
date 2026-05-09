from marshmallow import Schema, fields


class DistributionSchema(Schema):
    id = fields.Integer(dump_only=True)
    slug = fields.String()
    label_en = fields.String()
    label_pt = fields.String()
