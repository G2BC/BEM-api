import os


def _private() -> dict:
    return {"hidden": True} if os.getenv("APP_ENV", "development").strip().lower() != "development" else {}


def register_blueprints(api) -> None:
    from .auth_routes import auth_bp
    from .contact_routes import contact_bp
    from .reference_routes import reference_bp
    from .registration_routes import registration_bp
    from .snapshot_routes import snapshot_bp
    from .species_change_request_routes import (
        admin_species_change_request_bp,
        species_change_request_bp,
    )
    from .species_routes import admin_specie_bp, specie_bp
    from .user_routes import user_bp

    api.register_blueprint(auth_bp, url_prefix="/auth", spec_opts=_private())
    api.register_blueprint(registration_bp, url_prefix="/registrations", spec_opts=_private())
    api.register_blueprint(specie_bp, url_prefix="/species")
    api.register_blueprint(species_change_request_bp, url_prefix="/species-change-requests", spec_opts=_private())
    api.register_blueprint(contact_bp, url_prefix="/contact-messages", spec_opts=_private())
    api.register_blueprint(snapshot_bp, url_prefix="/snapshots")
    api.register_blueprint(user_bp, url_prefix="/admin/users", spec_opts=_private())
    api.register_blueprint(admin_specie_bp, url_prefix="/admin/species", spec_opts=_private())
    api.register_blueprint(
        admin_species_change_request_bp,
        url_prefix="/admin/species-change-requests",
        spec_opts=_private(),
    )
    api.register_blueprint(reference_bp, url_prefix="/admin/references", spec_opts=_private())
