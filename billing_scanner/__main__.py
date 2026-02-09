import click
from eodhp_utils.runner import log_component_version, setup_logging

from billing_scanner.scanner import BillingScanner


@click.command()
@click.option("-v", "--verbose", count=True, help="Set log verbosity.")
def cli(verbose: int = 1) -> None:
    setup_logging(verbosity=verbose)
    log_component_version("eodhp-accounting-cloudfront")
    scanner = BillingScanner()
    scanner.run()


if __name__ == "__main__":
    cli()
