# mariadb-postgres-migration
Migrates the database from w0bm v1 (mariadb) to v2 (postgres)

## Usage
1. `git clone --rescurse-submodules git://github.com/w0bm/mariadb-postgres-migration.git`
2. In `config.json`
    * Enter the database credentials of both databases
    * As passwords can't be decrypted, set a placeholder that will be inserted into the new password column
    * Set a tag seperator for the tag select query, must be something unique that isn't present in any other tag
3. `npm i && npm start`
