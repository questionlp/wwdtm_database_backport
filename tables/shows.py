# Copyright (c) 2025 Linh Pham
# wwdtm_database_backport is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Database Backport: Shows Table."""
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection


class Shows:
    """Wait Wait Stats Database Shows Table.

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
            SELECT showid, showdate, repeatshowid, bestof, bestofuniquebluff
            FROM ww_shows
            ORDER BY showid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        # Loop through all show entries, but do not fill in repeatshowid
        # column due to constraint
        for show in source_data:
            query = """
                INSERT INTO ww_shows (showid, showdate, bestof, bestofuniquebluff)
                VALUES (%s, %s, %s, %s);
            """
            destination_cursor.execute(
                query,
                (
                    show["showid"],
                    show["showdate"],
                    show["bestof"],
                    show["bestofuniquebluff"],
                ),
            )

        # Loop through show entries and only update existing rows to set
        # repeatshowid if not None
        for show in source_data:
            if show["repeatshowid"]:
                query = """
                    UPDATE ww_shows SET repeatshowid = %s
                    WHERE showid = %s;
                """
                destination_cursor.execute(
                    query,
                    (
                        show["repeatshowid"],
                        show["showid"],
                    ),
                )

        destination_cursor.close()
        return
