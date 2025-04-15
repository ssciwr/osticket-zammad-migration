# osticket-zammad-migration

This script exports all tickets from osticket and re-creates them in Zammad.

This script needs to run on `ssc-ticket` (where the osticket mysql database is running) with these env vars set:

- `OSTICKET_PASSWORD` set to the mysql password for database user `liam`
- `ZAMMAD_TOKEN` set to a HTTP access token for zammad user `liam.keegan@iwr.uni-heidelberg.de`

To run:

- `pip install .`
- `osticket-zammad-migration`

```bash
(migration) liam@ssc-ticket:~$ osticket-zammad-migration
Delete all existing Zammad tickets? [y/N]: y
Dry run without making any changes? [Y/n]: n
Do you want to continue? [Y/n]: y
#243974:  58%|███████████████████████████████▍                      | 1416/2430 [10:55<05:42,  2.96it/s]
```

Logs are written to `osticket-zammad-migration.log`.
