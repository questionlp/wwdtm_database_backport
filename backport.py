# Copyright (c) 2025 Linh Pham
# wwdtm_database_backport is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Database Backport."""
import json
from pathlib import Path

from tables.descriptions import Descriptions
from tables.guests import Guests
from tables.hosts import Hosts
from tables.locations import Locations
from tables.mappings import AllMappings
from tables.notes import Notes
from tables.panelists import Panelists
from tables.scorekeepers import Scorekeepers
from tables.shows import Shows


def load_config(config_file: str = "config.json") -> dict[str, str | int | None] | None:
    """Load database configuration for source and destination databases."""
    config_path: Path = Path.cwd() / config_file
    with config_path.open(mode="r", encoding="utf-8") as _config_file:
        _config_keys = json.load(_config_file)
        if not _config_keys:
            return None

        if not isinstance(_config_keys, dict):
            return None

    if not all(
        key in _config_keys for key in ("source_database", "destination_database")
    ):
        return None

    return _config_keys


def transfer_data(
    source_database_config: dict, destination_database_config: dict
) -> None:
    """Process and transfer data from newer database to older database versions."""
    _shows = Shows(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _descriptions = Descriptions(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _notes = Notes(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _guests = Guests(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _hosts = Hosts(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _locations = Locations(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _panelists = Panelists(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _scorekeepers = Scorekeepers(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )
    _all_mappings = AllMappings(
        source_connect_dict=source_database_config,
        destination_connect_dict=destination_database_config,
    )

    _shows.transfer()
    _descriptions.transfer()
    _notes.transfer()
    _guests.transfer()
    _hosts.transfer()
    _locations.transfer()
    _panelists.transfer()
    _scorekeepers.transfer()
    _all_mappings.transfer_all()


def main() -> None:
    """Main application entry point."""
    _config_keys: dict = load_config()
    if _config_keys:
        transfer_data(
            source_database_config=_config_keys["source_database"],
            destination_database_config=_config_keys["destination_database"],
        )


if __name__ == "__main__":
    main()
