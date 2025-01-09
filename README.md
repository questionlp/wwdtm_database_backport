# Wait Wait Stats Database Backport

This Python application pulls data from a copy of the [Wait Wait Stats Database](https://github.com/questionlp/wwdtm_database) based version 4.x, processes and imports the data into a freshly initiated instance of a Wait Wait Stats Database based on version 3.

By backporting the data to a previous version of the Stats Database, earlier versions of the [Wait Wait Stats Page](https://stats.wwdt.me/) application could be used with recent data.

## Background Information

Prior to version 4.1, the Wait Wait Stats Database used the `utf8` MySQL/MariaDB character set, which was an alias for `utf8mb3` and not `utf8mb4` as one would have assumed. This creates an issue with the inability to store characters that require 4 bytes per code point.

This wasn't an initial issue when the Stats Page was written for PHP 5 and 6, due to the middling or complete lack of proper Unicode support. When the application was re-written for Python 3, the database had to be updated to use the `utf8mb4` character set. While this doesn't completely break older versions of the Stats Page, it did lead to issues of accented and compound characters to either cause the PHP-based application to error out or not render out strings containing such characters.

When the application encounters a string or text field, it will run `unicodedata.normalize` against the string and reduce accented characters and compound characters to the base ASCII representation as part of its processing. This prevents issues with PHP incorrectly rendering strings or omitting them completely.

## Usage

In order to use this application, you will need a copy of the Wait Wait Stats Database based on version 4.2 or later running on MySQL 8 or newer and an instance of MySQL 5.6 or MariaDB 10 or newer as a destination. Older versions of the Wait Wait Stats Page application **do not** support versions of MySQL higher than 5.6 due to multiple breaking changes implemented in newer versions.

On the destination database server, create a clean database and run the [wwdtm.initial.sql](https://github.com/questionlp/wwdtm_database/blob/main/v3/wwdtm.initial.sql) database intialization script for Wait Wait Stats Database version 3. You will have to modify the script to replace instances of ``lpham`@`%`` in the view definitions with a user with the appropriate privileges to create databases, tables and views.

Once the database has been created and initialized, clone this repository, set up a virtual environment, install the dependencies from `requirements.txt`.

Next, make a copy of the included `config.dist.json` file and name the copy `config.json` and fill in the required database connection information for both the source and destination databases.

When the above steps have been completed, the application can be run using the following command:

```bash
python3 backport.py
```

## Contributing

If you would like contribute to this project, please make sure to review the [Code of Conduct](CODE_OF_CONDUCT.md) included in this repository.

## License

This application is licensed under the terms of the [Apache License 2.0](http://www.apache.org/licenses/LICENSE-2.0).
