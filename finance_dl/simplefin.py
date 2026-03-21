"""Retrieves data from Simplefin-bridge.

This uses the Simplefin API (https://www.simplefin.org/protocol.html) to
retrieve the data directly.

Initial Setup:
======
1. Go to https://beta-bridge.simplefin.org/ and add a new App connection.
2. Name it to your liking and click `Create Setup Token`
3. Copy the code to the clipboard

Option 1 - Handle the credentials yourself
4. From commandline, run `python <path to simplefin_access_token_setup.py> <access token>`.
5. The command will output the Access URL. Keep this in a safe place.
6. Supply the Access URL as the `access_url` argument for the configuration dict.

Option 2 - Let the script store it in your machine's keyring
4. Skip ahead to the configuration section and set the finance_dl config file up.
   Omit the `access_url` argument.
5. Run finances-dl with a configuration dictionary that omits `access_url`
6. When prompted, paste the Setup Token.
7. The access url will be retrieved and stored on the machine's keyring.

Configuration:
==============

The following keys may be specified as part of the configuration dict:

- `output_directory`: REQUIRED. Must be a `str` that specifies the path to the
  folder where the output will be written.  If the directory does not exist, it
  will be created.

- `access_url`: Optional. A string formatted according to Simplefin's 
  specifications. "https://...:...@beta-bridge.simplefin.org/simplefin". If this
  is omitted, you will be prompted for a new setup token, which then retrieves
  the access url and stores it on your system's keyring. If you need to define
  multiple configs for different simplefin accounts (or different sets of accounts),
  this argument must be supplied.

- `start_day`: Optional. First day for the date range. Formatted `YYYY-MM-DD`.
  Date range must not exceed {MAX_DAYS} days, per API limitations.

- `end_day`: Optional. Last day for the date range. Formatted `YYYY-MM-DD`.
  Date range must not exceed {MAX_DAYS} days, per API limitations.

- `archive_after_x_days`: Optional. Customize how much data to keep in the "live"
  transactions file. This ensures data is preserved in an archive file while not
  bogging down parser (i.e. beancount-import) with all transactions ever made.
  Only the non-ingested transactions are relevant to the parser, so set this as
  low as you're comfortable with to maximize performance (i.e. 30 days if you're
  sure you'll update at least once a month). Setting it to `None` disables this
  feature. NOTE: if `accounts` is specified, this only applies to those accounts.
  For example, if I already downloaded BANK_1 transactions from 2026-01-01 to
  2026-03-15 (today) but specify BANK_2 in the config with a 14 day archive window,
  BANK_2 transactions before 2026-03-02 will go to the archive, but BANK_1's txns
  will remain in the current file.

- `pending`: Optional. Query pending transactions from the API. Not recommended
  because this can create multiple entries for a single transaction; one ID when
  it is pending, and another when it officially lands.

- `accounts`: Optional. A string or list of strings representing the SimpleFin account
  ids that should be retrieved. Limits the retrieved data to those accounts. Previously
  downloaded transactions for other accounts will not be archived or adjusted.

- `balances_only`: Optional. Ignore transactions and holdings, only obtain balances
  from the API. Not very useful outside of debugging context because empty accounts
  are scrubbed from the outputs. It will only save balances for accounts that previously
  stored transactions.

- `ignore_fialed_accounts`: Optional. SimpleFin accounts can frequently be disconnected.
  Whether this matters is up to the user. Set this to true to grab whatever data is avail-
  able without throwing errors if some accounts cannot be reached.

- `debug`: Writes out an additional JSON file that represents the exact data returned by
  the API query. No transformations or sorting is applied.


Usage Details:
==============

The API expects less than 24 calls per day, will warn at 48, and will block at 96. See the
[developer docs](https://beta-bridge.simplefin.org/info/developers#Limits) and [this github
issue](https://github.com/actualbudget/actual/issues/3228#issuecomment-2387241284). To
respect these constraints, the scraper keeps a file on disk that prevents it from running
if it is called more than {RATE_LIMIT} times in a day.


Output format:
==============

All data is stored in a JSON file called {DATA_FILE_NAME}. Older transactions will be
transferred to {ARCHIVE_FILE_NAME}. The data structure mimics what the API returns, new
transactions are merged in as more data is pulled.


Example:
========

def CONFIG_simplefin():
    
    # Alter to your preferred credential management system
    kp = PyKeePass('C:/my_password_file.kdbx', keyfile='C:/MyDocuments/my_lock_file.txt')
    simplefin_creds = kp.find_entries(title='Simplefin', first=True)
    my_access_url = simplefin_creds.url  # "https://...:...@beta-bridge.simplefin.org/simplefin"
    
    return dict(
        module='finance_dl.simplefin',
        access_url=my_access_url,
        output_directory=os.path.join(data_dir, 'simplefin'),
        days=45,
        archive_days=180,
    )

"""

import re
import contextlib
import requests
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
import json
import pickle
from typing import Iterator, Optional, Union
from pathlib import Path
import logging
import finance_dl.simplefin_access_token_setup as sf_token


logger = logging.getLogger('simplefin_dl')


API_URL = "https://beta-bridge.simplefin.org/simplefin/accounts"
RATE_LIMIT_FILENAME = "rate_limits.pkl"  # {'date': <datetime>, 'counter': <int>}
RATE_LIMIT = 20
MAX_DAYS = 44  # set one lower than the official API max for safety
DEFAULT_ARCHIVE_AFTER_X_DAYS = 365
DATA_FILE_NAME = "simplefin_transactions.json"
ARCHIVE_FILE_NAME = "simplefin_transactions_archive.json"
DEBUG_FILE_NAME = r"simplefin_debug_{timestamp}.json"
DISCONNECTED_ACCOUNT_REGEX = r"Connection to .* may need attention\."


class SimplefinScraper:
    def __init__(
        self,
        output_directory: str,
        access_url: str = "",
        start_day: str = "",
        end_day: str = "",
        archive_after_x_days: Optional[int] = DEFAULT_ARCHIVE_AFTER_X_DAYS,
        pending: bool = False,
        accounts: Union[list[str], str] = list(),
        balances_only: bool = False,
        ignore_failed_accounts: bool = False,
        debug: bool = False,
        headless: bool = True,
    ) -> None:
        
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limit_file = self.output_dir / RATE_LIMIT_FILENAME
        self.ensure_rate_limit_file()
        
        self.start_date = format_date(start_day)
        self.end_date = format_date(end_day)

        # start_timestamp, end_timestamp = calculate_date_range(format_date(start_day), format_date(end_day))
        
        # logger.info(f'provided date range resolved to the following: {date.fromtimestamp(start_timestamp)}'
        #             f' to {date.fromtimestamp(end_timestamp)}')
        
        self.params = {
            'start-date': None,
            'end-date': None,
            'pending': pending,
            'balances-only': balances_only
        }

        # parse credentials
        parsed_url = urlparse(_retrieve_access_url(access_url))
        username = parsed_url.username
        password = parsed_url.password
        self.auth = (username, password)
        
        self.accounts = accounts
        if accounts:
            self.params['account'] = accounts
        
        self.ignore_failed = ignore_failed_accounts
        self._debug = debug

        self._archive_cutoff = self.get_archive_timestamp(archive_after_x_days)

        self._sort_function = lambda item: (
            item.get('posted', 0),  # for transactions
            item.get('symbol', ''),  # for holdings
            item.get('created', 0),  # for holdings
        )

    def ensure_rate_limit_file(self) -> None:
        if not self.rate_limit_file.is_file():
            with open(self.rate_limit_file, 'wb') as f:
                pickle.dump({'date': datetime.today().date(), 'counter': 0}, f)

    def verify_under_rate_limits(self) -> bool:
        with open(self.rate_limit_file, 'rb') as f:
            rates = pickle.load(f)

        if rates.get('date') != datetime.today().date():
            counter = 0
        else:
            counter = rates.get('counter')
        
        logger.info(f'Daily API calls: {counter}/{RATE_LIMIT}')
        return not counter > RATE_LIMIT
        
    def update_rate_limits(self) -> None:
        with open(self.rate_limit_file, 'rb') as f:
            old_data = pickle.load(f)
        
        today = datetime.today().date()
        if old_data.get('date') == today:
            old_data['counter'] += 1
            new_data = old_data
        else:
            new_data = {'date': today, 'counter': 1}
        
        with open(self.rate_limit_file, 'wb') as f:
            pickle.dump(new_data, f)
    
    def get_archive_timestamp(self, days) -> Optional[int]:
        if days:
            today = date.today()
            archive_datetime = today - timedelta(days=days)
            archive_date = datetime(archive_datetime.year, archive_datetime.month, archive_datetime.day)
            return int(archive_date.timestamp())
        
    def fetch_data(self) -> dict:
        response = requests.get(
            API_URL,
            params = self.params,
            auth = self.auth,
        )
        try:
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            msg = "Authentication failed. Incorrect or revoked credentials."
            sf_token.inform_user_about_response_codes(e, logger, msg)
            raise
        
        return response.json()
    
    def log_errors(self, data: dict) -> None:
        if not data.get('errors'):
            return
        
        critical_errors = list()

        if self.ignore_failed:
            logger.info("Ignore failed is enabled. Disconnected accounts will not be flagged as critical errors.")

        logger.info("SIMPLEFIN ERRORS DETECTED:")

        for error in data['errors']:
            disconnected_account_pattern_match = re.search(DISCONNECTED_ACCOUNT_REGEX, error)
            if disconnected_account_pattern_match and self.ignore_failed:
                logger.info(f"🔴 {error}")
            else:
                logger.error(error)
                critical_errors.append(error)

        if critical_errors:
            raise RuntimeError("Errors detected with registered Simplefin accounts. Please correct before proceeding!")
    
    def retrieve_old_data(self, data_file) -> dict:
        if data_file.is_file():
            with open(data_file, 'r') as f:
                update_data = json.load(f)
        else:
            update_data = dict()
        if not update_data:
            update_data = self._set_up_new_data()
        return update_data
        
    def save_new_data(self, new_data, data_file: Path) -> None:
        data_file.parent.mkdir(parents=True, exist_ok=True)
        with open(data_file, 'w') as f:
            json.dump(new_data, f, indent=4)
        
    def account_matches(self, first_account, second_account) -> bool:
        return first_account.get('id') == second_account.get('id')
    
    def merge_new_txn(self, account_list: list, new_txn: dict) -> None:
        for old_txn in account_list:
            if old_txn['id'] == new_txn['id']:
                return
        account_list.append(new_txn)
    
    def _get_account(self, data: list, account: dict) -> dict:
        for account_candidate in data:
            if self.account_matches(account_candidate, account):
                return account_candidate
        return self._set_up_new_account(data, account)
    
    def update_accounts_in_dicts(self, read_dict, current_dict, archive_dict) -> None:
        for account in read_dict['accounts']:
            if self.accounts and account['id'] not in self.accounts:
                continue
            archive_account = self._get_account(archive_dict['accounts'], account)
            current_account = self._get_account(current_dict['accounts'], account)
            self.update_txns_for_accounts(account, current_account, archive_account, 'transactions')
            self.update_txns_for_accounts(account, current_account, archive_account, 'holdings')
            self.update_balance_for_account(account, current_account)
            self.update_balance_for_account(account, archive_account)

    def update_txns_for_accounts(self, read_account, current_account, archive_account, key):
        """
        key: 'transactions' or 'holdings'
        """
        for txn in read_account[key]:
            txn_date = txn.get('transacted_at') or txn.get('created')
            if self._archive_cutoff is None or txn_date > self._archive_cutoff:
                self.merge_new_txn(current_account[key], txn)
            else:
                self.merge_new_txn(archive_account[key], txn)
        archive_account[key].sort(key=self._sort_function)
        current_account[key].sort(key=self._sort_function)
    
    def update_balance_for_account(self, read_account, write_account):
        for key in ('balance', 'available-balance', 'balance-date'):
            write_account[key] = read_account[key]
    
    def _set_up_new_data(self):
        template = {
            'errors': list(),
            'accounts': list()
        }
        return template
    
    def _set_up_new_account(self, data: list, account_info: dict) -> dict:
        transfer_keys = ('org', 'id', 'name', 'currency')
        template = dict()
        for key in transfer_keys:
            template[key] = account_info[key]
        
        template['transactions'] = list()
        template['holdings'] = list()
        
        data.append(template)
        return template
    
    def _scrub_empty_accounts(self, data):
        data['accounts'] = [acc for acc in data['accounts'] if acc.get('transactions') or acc.get('holdings')]
            
    def _restore_other_accounts(self, read_dict, write_dict):
        """
        All data is written to {DATA_FILE_NAME} and {ARCHIVE_FILE_NAME}, no matter how many
        SimpleFin configurations are defined in the finance-dl config file. This allows the
        user to use the same json files, but define multiple configurations for specific accounts.
        Useful if the user wants some accounts to keep longer archive windows than others, and/or
        to query the API for some accounts less frequently.
        """
        for account in read_dict['accounts']:
            if account['id'] not in self.accounts:
                write_dict['accounts'].append(account)

    def save_and_archive_data(self, data) -> None:
        data_file = self.output_dir / DATA_FILE_NAME
        archive_file = self.output_dir / ARCHIVE_FILE_NAME
        
        file_data = self.retrieve_old_data(data_file)  # might contain archivable data
        archive_data = self.retrieve_old_data(archive_file)  # adjustments to archive_days may make some of this data "current"
        
        current_dict = self._set_up_new_data()
        archive_dict = self._set_up_new_data()
        
        logger.info(f"Loading archive data from {archive_file}...")
        self.update_accounts_in_dicts(archive_data, current_dict, archive_dict)
        
        logger.info(f"Loading existing data from {data_file}...")
        self.update_accounts_in_dicts(file_data, current_dict, archive_dict)
        
        logger.info("Ingesting feched data...")
        self.update_accounts_in_dicts(data, current_dict, archive_dict)
        
        current_dict['errors'] = data.get('errors')
        
        if self.accounts:
            self._restore_other_accounts(file_data, current_dict)
            self._restore_other_accounts(archive_data, archive_dict)

        self._scrub_empty_accounts(current_dict)
        self._scrub_empty_accounts(archive_dict)
        
        logger.info(f"Saving data to {data_file}...")
        self.save_new_data(current_dict, data_file)
        logger.info(f"Saving data to {archive_file}...")
        self.save_new_data(archive_dict, archive_file)
    
    def fetch_and_save_data(self) -> None:
        start = datetime.fromtimestamp(self.params.get('start-date')).strftime("%Y-%m-%d")
        end = datetime.fromtimestamp(self.params.get('end-date')).strftime("%Y-%m-%d")
        logger.info(f"⚪ Fetching data from {start} to {end}")

        if not self.verify_under_rate_limits():
            raise RuntimeError(f"Simplefin scraper aborted: maximum {RATE_LIMIT} calls per day.")
        
        new_data = self.fetch_data()
        self.update_rate_limits()

        if self._debug:
            timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S%f")
            debug_file = self.output_dir / "_debug" / DEBUG_FILE_NAME.format(timestamp=timestamp)
            logger.info(f"saving debug file to {debug_file}")
            self.save_new_data(new_data, debug_file)
        
        self.log_errors(new_data)
        self.save_and_archive_data(new_data)

        logger.info(f"🟢 Data from {start} to {end} successfully saved")
        

    def run(self) -> None:
        logger.info(f"Attempting to fetch data from {API_URL}")
        logger.info(f"Starting api calls for {self.accounts or "all accounts"}")
        date_ranges = get_date_chunks(self.start_date, self.end_date)
        for start, end in date_ranges:
            self.params['start-date'] = start
            self.params['end-date'] = end
            self.fetch_and_save_data()
        logger.info(f"Success, fetched all data!")
        

def _retrieve_access_url(url):
    if not url:
        url = sf_token.auto_setup()
    return url


def format_date(input: str) -> Optional[date]:
    try:
        return date.fromisoformat(input)
    except ValueError:
        return None


def calculate_date_range(start_date: Optional[date], end_date: Optional[date]) -> tuple[int, int]:
    """
    Flexibly deterime the date range based on optional user inputs.
    
    Returns:
        (int, int) representing two timestamps, the start time and the end time.
    
    Raises:
        ValueError if the date range exceeds {MAX_DAYS} days.
        ValueError if the date range is negative.
        ValueError if the start date is in the future (no valid data to fetch)
        ValueError if the end date is more than {MAX_DAYS - 1} days in the future (no valid data to fetch)
    """
    today = date.today()
    if start_date and end_date:
        delta = end_date - start_date
        if delta.days > MAX_DAYS:
            msg = "SimpleFin API limitations dictate start and end days must not span more than {MAX_DAYS} days."
            msg += f"Provided days span {delta} days."
            raise ValueError(msg)
        if delta.days < 0:
            msg = f"End date ({end_date}) must be before start date ({start_date})."
            raise ValueError(msg)
    
    elif start_date and not end_date:
        if start_date > today:
            msg = f"Start date ({start_date}) cannot be in the future. Current date: {today}"
            raise ValueError(msg)
        
        end_date = start_date + timedelta(days=MAX_DAYS)
            
    elif not start_date and end_date:
        start_date = end_date - timedelta(days=MAX_DAYS)
        
        if start_date > today:
            msg = f"End date ({end_date}) cannot be more than {MAX_DAYS - 1} days in the future. Current date: {today}"
            raise ValueError(msg)
    
    else:
        end_date = date.today()
        start_date = end_date - timedelta(days=MAX_DAYS)
    
    if end_date > today:
        end_date = today
    
    start_date = datetime(start_date.year, start_date.month, start_date.day)
    end_date = datetime(end_date.year, end_date.month, end_date.day)
    start_timestamp = int(start_date.timestamp())
    end_timestamp = int(end_date.timestamp())
    
    return start_timestamp, end_timestamp


def get_date_chunks(start_date: Optional[date], end_date: Optional[date]) -> Iterator[tuple[int, int]]:
    today = date.today()

    if not start_date and not end_date:
        end_date = today
        start_date = end_date - timedelta(days=MAX_DAYS)
    elif start_date and not end_date:
        end_date = min(start_date + timedelta(days=MAX_DAYS), today)
    elif not start_date and end_date:
        start_date = end_date - timedelta(days=MAX_DAYS)

    if start_date > end_date:
        msg = f"End date ({end_date}) must be after start date ({start_date})."
        raise ValueError(msg)
    if start_date > today:
        msg = f"Start date ({start_date}) cannot be in the future."
        raise ValueError(msg)
    
    current_end = end_date

    while current_end > start_date:
        current_start = max(current_end - timedelta(days=MAX_DAYS), start_date)

        converted_start_date = datetime(current_start.year, current_start.month, current_start.day)
        converted_end_date = datetime(current_end.year, current_end.month, current_end.day)

        start_timestamp = int(converted_start_date.timestamp())
        end_timestamp = int(converted_end_date.timestamp())

        yield start_timestamp, end_timestamp

        current_end = current_start


def run(**kwargs):
    scraper = SimplefinScraper(**kwargs)
    scraper.run()


@contextlib.contextmanager
def interactive(**kwargs):
    scraper = SimplefinScraper(**kwargs)
    kwargs['scraper'] = scraper
    yield kwargs