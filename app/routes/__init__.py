import os

from flask import Flask
from flask_smorest import Api


def register_blueprints(api: Api, app: Flask) -> None:
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

    is_dev = os.getenv("APP_ENV", "development").strip().lower() == "development"
    private = api.register_blueprint if is_dev else app.register_blueprint

    api.register_blueprint(specie_bp, url_prefix="/species")
    api.register_blueprint(snapshot_bp, url_prefix="/snapshots")
    private(auth_bp, url_prefix="/auth")
    private(registration_bp, url_prefix="/registrations")
    private(species_change_request_bp, url_prefix="/species-change-requests")
    private(contact_bp, url_prefix="/contact-messages")
    private(user_bp, url_prefix="/admin/users")
    private(admin_specie_bp, url_prefix="/admin/species")
    private(admin_species_change_request_bp, url_prefix="/admin/species-change-requests")
    private(reference_bp, url_prefix="/admin/references")
