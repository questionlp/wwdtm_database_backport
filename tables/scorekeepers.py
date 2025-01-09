# Copyright (c) 2025 Linh Pham
# wwdtm_database_backport is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Database Backport: Scorekeepers Table."""
import unicodedata
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection


class Scorekeepers:
    """Wait Wait Stats Database Scorekeepers Table.

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
            SELECT scorekeeperid, scorekeeper, scorekeepergender, scorekeeperslug
            FROM ww_scorekeepers
            ORDER BY scorekeeperid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        for scorekeeper in source_data:
            query = """
                INSERT INTO ww_scorekeepers
                (scorekeeperid, scorekeeper, scorekeepergender, scorekeeperslug)
                VALUES (%s, %s, %s, %s);
            """
            if scorekeeper["scorekeeper"]:
                scorekeeper_name = (
                    unicodedata.normalize("NFKD", scorekeeper["scorekeeper"])
                    .encode(encoding="ASCII", errors="ignore")
                    .decode(encoding="utf-8")
                )
            else:
                scorekeeper_name = None

            destination_cursor.execute(
                query,
                (
                    scorekeeper["scorekeeperid"],
                    scorekeeper_name,
                    scorekeeper["scorekeepergender"],
                    scorekeeper["scorekeeperslug"],
                ),
            )

        destination_cursor.close()
        return
