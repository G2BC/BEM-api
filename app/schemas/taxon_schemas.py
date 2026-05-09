from marshmallow import Schema, fields


class TaxonSchema(Schema):
    classification = fields.String(allow_none=True, dump_only=True)
    synonyms = fields.String(allow_none=True, dump_only=True)
    basionym = fields.String(allow_none=True, dump_only=True)
    authors = fields.String(allow_none=True, dump_only=True)
    gender = fields.String(allow_none=True, dump_only=True)
    name_status = fields.String(allow_none=True, dump_only=True)
    years_of_effective_publication = fields.String(allow_none=True, dump_only=True)
