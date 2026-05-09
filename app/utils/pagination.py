from app.exceptions import AppError
from marshmallow import Schema, fields


def resolve_page_params(page, per_page, default_per_page=20, max_per_page=100):
    """Validate and resolve page/per_page params.

    Returns (None, None) when both are None (caller should return unpaginated list).
    Otherwise returns (page, per_page) with defaults applied and validated.
    Raises AppError on invalid values.
    """
    if page is None and per_page is None:
        return None, None

    if page is None:
        page = 1
    if per_page is None:
        per_page = default_per_page

    if not isinstance(page, int) or page < 1:
        raise AppError(pt="`page` deve ser um inteiro >= 1", en="`page` must be an integer >= 1")
    if not isinstance(per_page, int) or per_page < 1:
        raise AppError(
            pt="`per_page` deve ser um inteiro >= 1", en="`per_page` must be an integer >= 1"
        )
    if per_page > max_per_page:
        raise AppError(
            pt=f"`per_page` deve ser <= {max_per_page}",
            en=f"`per_page` must be <= {max_per_page}",
        )

    return page, per_page


def build_page_response(result, page, per_page):
    """Build the standard paginated response dict.

    When page is None, result must be a plain list (unpaginated).
    Otherwise result must be a SQLAlchemy Pagination object.
    """
    if page is None:
        return {
            "items": result,
            "total": len(result),
            "page": None,
            "per_page": None,
            "pages": None,
        }
    return {
        "items": result.items,
        "total": result.total,
        "page": page,
        "per_page": per_page,
        "pages": result.pages,
    }


def make_pagination_schema(item_schema_class):
    """Return a Marshmallow Schema class for paginated responses of the given item type."""
    item_name = item_schema_class.__name__.removesuffix("Schema")

    schema = type(
        f"{item_name}PaginationSchema",
        (Schema,),
        {
            "items": fields.List(fields.Nested(item_schema_class)),
            "total": fields.Integer(),
            "page": fields.Integer(allow_none=True),
            "per_page": fields.Integer(allow_none=True),
            "pages": fields.Integer(allow_none=True),
        },
    )
    return schema
