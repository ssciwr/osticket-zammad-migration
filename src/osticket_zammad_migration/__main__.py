from .zammad import migrate_tickets
import click
import logging


@click.command()
def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s :: %(levelname)s :: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler("osticket-zammad-migration.log")],
    )
    delete_all_existing_tickets = click.confirm(
        "Delete all existing Zammad tickets?", default=False
    )
    dry_run = click.confirm("Dry run without making any changes?", default=True)
    click.confirm("Do you want to continue?", abort=True, default=True)
    migrate_tickets(delete_all_existing_tickets, dry_run)


if __name__ == "__main__":
    main()
