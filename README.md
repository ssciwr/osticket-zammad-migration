# osticket-zammad-migration

This script exports all tickets from osticket and re-creates them in zammad.

This script needs to run on `ssc-ticket` (where the osticket mysql database is running) with these env vars set:

- `OSTICKET_PASSWORD` set to the mysql password for database user `liam`
- `ZAMMAD_TOKEN` set to a HTTP access token for zammad user `liam.keegan@iwr.uni-heidelberg.de`

To run:

- pip install .
- osticket-zammad-migration

```bash
Usage: osticket-zammad-migration [OPTIONS]

Options:
  --delete-all-existing-tickets  Delete all existing tickets in Zammad before
                                 migrating.
  --dry-run                      Perform a dry run without making any changes.
  --help                         Show this message and exit.
```
