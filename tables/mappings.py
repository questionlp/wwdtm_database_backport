# Copyright (c) 2025 Linh Pham
# wwdtm_database_backport is released under the terms of the Apache License 2.0
# SPDX-License-Identifier: Apache-2.0
#
# vim: set noai syntax=python ts=4 sw=4:
"""Wait Wait Stats Database Backport: Mapping Tables."""
import unicodedata
from typing import Any

from mysql.connector import connect
from mysql.connector.connection import MySQLConnection


class Bluffs:
    """Wait Wait Stats Database Bluff the Listener Mappings Table.

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
            SELECT showbluffmapid, showid, chosenbluffpnlid, correctbluffpnlid
            FROM ww_showbluffmap
            WHERE segment = 1
            ORDER BY showbluffmapid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        for bluff in source_data:
            query = """
                INSERT INTO ww_showbluffmap
                (showbluffmapid, showid, chosenbluffpnlid, correctbluffpnlid)
                VALUES (%s, %s, %s, %s);
            """
            destination_cursor.execute(
                query,
                (
                    bluff["showbluffmapid"],
                    bluff["showid"],
                    bluff["chosenbluffpnlid"],
                    bluff["correctbluffpnlid"],
                ),
            )

        destination_cursor.close()
        return


class Guests:
    """Wait Wait Stats Database Not My Job Guest Mappings Table.

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
            SELECT showguestmapid, showid, guestid, guestscore, exception
            FROM ww_showguestmap
            ORDER BY showguestmapid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        for guest in source_data:
            query = """
                INSERT INTO ww_showguestmap
                (showguestmapid, showid, guestid, guestscore, exception)
                VALUES (%s, %s, %s, %s, %s);
            """
            destination_cursor.execute(
                query,
                (
                    guest["showguestmapid"],
                    guest["showid"],
                    guest["guestid"],
                    guest["guestscore"],
                    guest["exception"],
                ),
            )

        destination_cursor.close()
        return


class Hosts:
    """Wait Wait Stats Database Host Mappings Table.

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
            SELECT showhostmapid, showid, hostid, guest
            FROM ww_showhostmap
            ORDER BY showhostmapid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        for host in source_data:
            query = """
                INSERT INTO ww_showhostmap
                (showhostmapid, showid, hostid, guest)
                VALUES (%s, %s, %s, %s);
            """
            destination_cursor.execute(
                query,
                (
                    host["showhostmapid"],
                    host["showid"],
                    host["hostid"],
                    host["guest"],
                ),
            )

        destination_cursor.close()
        return


class Locations:
    """Wait Wait Stats Database Location Mappings Table.

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
            SELECT showlocationmapid, showid, locationid
            FROM ww_showlocationmap
            ORDER BY showlocationmapid ASC;
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
                INSERT INTO ww_showlocationmap
                (showlocationmapid, showid, locationid)
                VALUES (%s, %s, %s);
            """
            destination_cursor.execute(
                query,
                (
                    location["showlocationmapid"],
                    location["showid"],
                    location["locationid"],
                ),
            )

        destination_cursor.close()
        return


class Panelists:
    """Wait Wait Stats Database Panelist Mappings Table.

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
            SELECT showpnlmapid, showid, panelistid, panelistlrndstart,
            panelistlrndcorrect, panelistscore, showpnlrank
            FROM ww_showpnlmap
            ORDER BY showpnlmapid ASC;
        """
        source_cursor.execute(query)
        source_data = source_cursor.fetchall()
        source_cursor.close()

        if not source_data:
            return

        destination_cursor = self.destination_database_connection.cursor(
            dictionary=True
        )

        for panelist in source_data:
            query = """
                INSERT INTO ww_showpnlmap
                (showpnlmapid, showid, panelistid, panelistlrndstart,
                panelistlrndcorrect, panelistscore, showpnlrank)
                VALUES (%s, %s, %s, %s, %s, %s, %s);
            """
            if panelist["showpnlrank"]:
                rank = (
                    unicodedata.normalize("NFKD", panelist["showpnlrank"])
                    .encode(encoding="ASCII", errors="ignore")
                    .decode(encoding="utf-8")
                )
            else:
                rank = None

            destination_cursor.execute(
                query,
                (
                    panelist["showpnlmapid"],
                    panelist["showid"],
                    panelist["panelistid"],
                    panelist["panelistlrndstart"],
                    panelist["panelistlrndcorrect"],
                    panelist["panelistscore"],
                    rank,
                ),
            )

        destination_cursor.close()
        return


class Scorekeepers:
    """Wait Wait Stats Database Scorekeeper Mappings Table.

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
            SELECT showskmapid, showid, scorekeeperid, guest, description
            FROM ww_showskmap
            ORDER BY showskmapid ASC;
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
                INSERT INTO ww_showskmap
                (showskmapid, showid, scorekeeperid, guest, description)
                VALUES (%s, %s, %s, %s, %s);
            """
            if scorekeeper["description"]:
                description = (
                    unicodedata.normalize("NFKD", scorekeeper["description"])
                    .encode(encoding="ASCII", errors="ignore")
                    .decode(encoding="utf-8")
                )
            else:
                description = None

            destination_cursor.execute(
                query,
                (
                    scorekeeper["showskmapid"],
                    scorekeeper["showid"],
                    scorekeeper["scorekeeperid"],
                    scorekeeper["guest"],
                    description,
                ),
            )

        destination_cursor.close()
        return


class AllMappings:
    """Wait Wait Stats Database All Mappings Table.

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

    def transfer_all(self) -> None:
        """Process and transfer all mapping tables from source to destination databases."""
        _bluffs = Bluffs(
            source_database_connection=self.source_database_connection,
            destination_database_connection=self.destination_database_connection,
        )
        _guests = Guests(
            source_database_connection=self.source_database_connection,
            destination_database_connection=self.destination_database_connection,
        )
        _hosts = Hosts(
            source_database_connection=self.source_database_connection,
            destination_database_connection=self.destination_database_connection,
        )
        _locations = Locations(
            source_database_connection=self.source_database_connection,
            destination_database_connection=self.destination_database_connection,
        )
        _panelists = Panelists(
            source_database_connection=self.source_database_connection,
            destination_database_connection=self.destination_database_connection,
        )
        _scorekeepers = Scorekeepers(
            source_database_connection=self.source_database_connection,
            destination_database_connection=self.destination_database_connection,
        )

        _bluffs.transfer()
        _guests.transfer()
        _hosts.transfer()
        _locations.transfer()
        _panelists.transfer()
        _scorekeepers.transfer()
