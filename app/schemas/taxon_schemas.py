from marshmallow import Schema, fields


class TaxonSchema(Schema):
    classification_raw = fields.String(allow_none=True, dump_only=True)
    kingdom = fields.String(allow_none=True, dump_only=True)
    phylum = fields.String(allow_none=True, dump_only=True)
    class_name = fields.String(allow_none=True, dump_only=True)
    order = fields.String(allow_none=True, dump_only=True)
    family = fields.String(allow_none=True, dump_only=True)
    genus = fields.String(allow_none=True, dump_only=True)
    section = fields.String(allow_none=True, dump_only=True)
    group = fields.String(allow_none=True, dump_only=True)
    specific_epithet = fields.String(allow_none=True, dump_only=True)
    synonyms = fields.String(allow_none=True, dump_only=True)
    basionym = fields.String(allow_none=True, dump_only=True)
    authors = fields.String(allow_none=True, dump_only=True)
    gender = fields.String(allow_none=True, dump_only=True)
    years_of_effective_publication = fields.String(allow_none=True, dump_only=True)
