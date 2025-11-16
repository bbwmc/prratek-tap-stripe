# tap-stripe
[![test](https://github.com/prratek/tap-stripe/actions/workflows/test.yml/badge.svg)](https://github.com/prratek/tap-stripe/actions/workflows/test.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

`tap-stripe` is a Singer tap for Stripe built with the [Singer SDK](https://gitlab.com/meltano/singer-sdk).

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install tap-stripe
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the tap.

A full list of supported settings and capabilities for this
tap is available by running:

```bash
tap-stripe --about
```

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your tap requires special access on the source system, or any special authentication requirements, provide those here.

## Usage

You can easily run `tap-stripe` by itself or in a pipeline using [Meltano](www.meltano.com).

### Linking Payouts, Transactions, and Destinations

- The `payouts` stream now pulls payout objects directly from Stripe and automatically expands the `destination` object so you can see the underlying bank account or card details without extra requests.
- The `balance_transactions` stream continues to ingest the global ledger, and now additionally retrieves the balance transactions associated with each payout (`payout=<id>`). Every emitted balance transaction includes the `payout` reference when it exists, enabling joins from `charges` → `balance_transactions` → `payouts` → destinations.
- No extra configuration is required—ensure the tap has a sufficiently old `start_date` to cover the payouts and balance activity you need to reconcile.

### Executing the Tap Directly

```bash
tap-stripe --version
tap-stripe --help
tap-stripe --config CONFIG --discover > ./catalog.json
```

## Developer Resources

- [ ] `Developer TODO:` As a first step, scan the entire project for the text "`TODO:`" and complete any recommended steps, deleting the "TODO" references once completed.

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `tap_stripe/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `tap-stripe` CLI interface directly using `poetry run`:

```bash
poetry run tap-stripe --help
```

### Testing with [Meltano](meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Your project comes with a custom `meltano.yml` project file already created. Open the `meltano.yml` and follow any _"TODO"_ items listed in
the file.

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-stripe
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-stripe --version
# OR run a test `elt` pipeline:
meltano etl tap-stripe target-jsonl
```

### Singer SDK Dev Guide

See the [dev guide](../../docs/dev_guide.md) for more instructions on how to use the Singer SDK to 
develop your own taps and targets.
