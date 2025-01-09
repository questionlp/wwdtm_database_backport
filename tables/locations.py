# Copyright (c) 2025 Linh Pham
# wwdtm_database_backport is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Database Backport: Locations Table."""
import unicodedata
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection


class Locations:
    """Wait Wait Stats Database Locations Table.

    This class contains database methods used to process and transfer
    data from the latest version of the Wait Wait Stats Database to
    a database set up for version 3.0.

    :param connect_dict: Dictionary containing database connection
        settings as required by mysql.connector.connect
    :param database_connection: mysql.connector.connect database
        connection
    """

    def __init__(
        self,
        source_connect_dict: dict[str, Any] | None = None,
        destination_connect_dict: dict[str, Any] | None = None,
        source_database_connection: MySQLConnection | None = None,
        destination_database_connection: MySQLConnection | None = None,
    ) -> None:
        """Class initialization method."""
        if source_connect_dict and destination_connect_dict:
            self.source_connect_dict = source_connect_dict
            self.destination_connect_dict = destination_connect_dict

            self.source_database_connection = connect(**source_connect_dict)
            self.destination_database_connection = connect(**destination_connect_dict)
        elif source_database_connection and destination_database_connection:
            if not source_database_connection.is_connected():
                source_database_connection.reconnect()

            if not destination_database_connection.is_connected():
                destination_database_connection.reconnect()

            self.source_database_connection = source_database_connection
            self.destination_database_connection = destination_database_connection

    def __str__(self):
        pass

    def transfer(self) -> None:
        """Process and transfer data from source to destination databases."""
        source_cursor = self.source_database_connection.cursor(dictionary=True)

        query = """
            SELECT locationid, city, state, venue, locationslug
            FROM ww_locations
            ORDER BY locationid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        for location in source_data:
            query = """
                INSERT INTO ww_locations (locationid, city, state, venue, locationslug)
                VALUES (%s, %s, %s, %s, %s);
            """
            if location["venue"]:
                venue_name = (
                    unicodedata.normalize("NFKD", location["venue"])
                    .encode(encoding="ASCII", errors="ignore")
                    .decode(encoding="utf-8")
                )
            else:
                venue_name = None

            destination_cursor.execute(
                query,
                (
                    location["locationid"],
                    location["city"],
                    location["state"],
                    venue_name,
                    location["locationslug"],
                ),
            )

        destination_cursor.close()
        return
