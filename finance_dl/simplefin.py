"""Retrieves data from Simplefin-bridge.

This uses the Simplefin API (https://www.simplefin.org/protocol.html) to
retrieve the data directly.

Configuration:
==============

The following keys may be specified as part of the configuration dict:

- `access_url`: Required.  Must be a string formatted according to Simplefin's 
  specifications. "https://...:...@beta-bridge.simplefin.org/simplefin"

- `output_directory`: Required.  Must be a `str` that specifies the path on the
  local filesystem where the output will be written.  If the directory does not
  exist, it will be created.

- `start_day`: Optional. First day for the date range. Formatted `YYYY-MM-DD`.
  Date range must not exceed {MAX_DAYS} days, per API limitations.

- `end_day`: Optional. Last day for the date range. Formatted `YYYY-MM-DD`.
  Date range must not exceed {MAX_DAYS} days, per API limitations.




- `dir_per_year`: Optional. If true (default is false), adds one subdirectory
  to the output for each year's worth of transactions. Useful for filesystems
  that struggle with very large directories. Probably not that useful for
  actually finding anything, given the uselessness of Amazon's order ID
  scheme.

- `amazon_domain`: Optional.  Specifies the Amazon domain from which to download
  orders.  Must be one of `'.com'`, `'.co.cuk'` or `'.de'`.  Defaults to
  `'.com'`.

- `regular`: Optional.  Must be a `bool`.  If `True` (the default), download regular orders.
   For domains other than `amazon_domain=".com"`, `True` downloads regular AND digital orders.

- `digital`: Optional.  Must be a `bool` or `None`.  If `True`, download digital
  orders. Effective only for `amazon_domain=".com"`. Defaults to `True` for
  `amazon_domain=".com"`. For other domains, digital invoices are downloaded
  tgehter with regular invoices since there is no separate menu on the amazon website.

- `profile_dir`: Optional.  If specified, must be a `str` that specifies the
  path to a persistent Chrome browser profile to use.  This should be a path
  used solely for this single configuration; it should not refer to your normal
  browser profile.  If not specified, a fresh temporary profile will be used
  each time.

- `order_groups`: Optional.  If specified, must be a list of strings specifying the Amazon
  order page "order groups" that will be scanned for orders to download. Order groups
  include years (e.g. '2020'), as well as 'last 30 days' and 'past 3 months'.

- `download_preorder_invoices`: Optional. If specified and True, invoices for
  preorders (i.e. orders that have not actually been charged yet) will be
  skipped. Such preorder invoices are not typically useful for accounting
  since they claim a card was charged even though it actually has not been
  yet; they get replaced with invoices containing the correct information when
  the order is actually fulfilled.


Usage Details:
==============

The API expects less than 24 calls per day, will warn at 48, and will block at 96. See the
[developer docs](https://beta-bridge.simplefin.org/info/developers#Limits) and [this github
issue](https://github.com/actualbudget/actual/issues/3228#issuecomment-2387241284). To
respect these constraints, the scraper keeps a file on disk that prevents it from running
if it is called more than 20 times in a day.



Output format:
==============

All data is stored in a JSON file called simplefin-transactions.json. Older transactions
will be transferred to archive-transactions.json.



Example:
========

def CONFIG_simplefin():
    
    # Alter to your preferred credential management system
    kp = PyKeePass(KEEPASS_DB, keyfile=key_file)
    simplefin_creds = kp.find_entries(title='Simplefin', first=True)
    my_access_url = simplefin_creds.url  # "https://...:...@beta-bridge.simplefin.org/simplefin"
    
    return dict(
        module='finance_dl.simplefin',
        access_url=my_access_url,
        output_directory=os.path.join(data_dir, 'simplefin'),
        days=45,
        archive_days=180,
    )

Interactive shell:
==================

????

"""

import contextlib
import requests
from datetime import datetime, date, timedelta
from urllib.parse import urlparse
import json
import pickle
from typing import Optional
from pathlib import Path
import logging


logger = logging.getLogger('simplefin_dl')


API_URL = "https://beta-bridge.simplefin.org/simplefin/accounts"
RATE_LIMIT_FILENAME = "rate_limits.pkl"  # {'date': <datetime>, 'counter': <int>}
RATE_LIMIT = 20
MAX_DAYS = 45
DATA_FILE_NAME = "simplefin_transactions.json"
ARCHIVE_FILE_NAME = "simplefin_transactions_archive.json"


class SimplefinScraper:
    def __init__(
        self,
        access_url: str,
        output_directory: str,
        start_day: str = "",
        end_day: str = "",
        archive_days: int = 365,
        pending: bool = False,
        account: Optional[str] = None,
        balances_only: bool = False,
        ignore_failed_acounts: bool = False,
        headless: bool = True,
    ) -> None:
        
        self.output_dir = Path(output_directory)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.rate_limit_file = self.output_dir / RATE_LIMIT_FILENAME
        self.ensure_rate_limit_file()
        
        self.access_url = access_url
        
        start_timestamp, end_timestamp = calculate_date_range(format_date(start_day), format_date(end_day))
        
        logger.info(f'provided date range resolved to the following: {date.fromtimestamp(start_timestamp)}'
                    f' to {date.fromtimestamp(end_timestamp)}')
        
        self.params = {
            'start-date': start_timestamp,
            'end-date': end_timestamp,
            'pending': pending,
            'balances-only': balances_only
        }
        
        if account:
            self.params['account'] = account
        
        self.ignore_failed = ignore_failed_acounts
        self._archive_cutoff = self.get_archive_timestamp(archive_days)
        self._sort_function = lambda item: item['posted']
    
    def ensure_rate_limit_file(self) -> None:
        if not self.rate_limit_file.is_file():
            with open(self.rate_limit_file, 'wb') as f:
                pickle.dump({'date': datetime.today().date(), 'counter': 0}, f)
    
    def verify_under_rate_limits(self) -> bool:
        with open(self.rate_limit_file, 'rb') as f:
            rates = pickle.load(f)
        counter = rates.get('counter')
        logger.info(f'Daily Attempts: {counter}/20')
        if rates.get('date') == datetime.today().date() and counter > 20:
            return False
        return True
        
    def update_rate_limits(self) -> None:
        with open(self.rate_limit_file, 'rb') as f:
            old_data = pickle.load(f)
        
        today = datetime.today().date()
        if old_data.get('date') == today:
            old_data['counter'] += 1
            new_data = old_data
        else:
            new_data = {'date': today, 'counter': 0}
        
        with open(self.rate_limit_file, 'wb') as f:
            pickle.dump(new_data, f)
    
    def get_archive_timestamp(self, days):
        today = date.today()
        archive_datetime = today - timedelta(days=days)
        archive_date = datetime(archive_datetime.year, archive_datetime.month, archive_datetime.day)
        return int(archive_date.timestamp())
    
    def fetch_data(self) -> dict:
        logger.info(f"Attempting to fetch data from {API_URL}")
        
        # parse credentials
        parsed_url = urlparse(self.access_url)
        username = parsed_url.username
        password = parsed_url.password
        
        auth = (username, password)
        response = requests.get(
            API_URL,
            params = self.params,
            auth = auth,
        )
        response.raise_for_status()
        
        logger.info("Data successfully fetched!")
        
        return response.json()
    
    def log_errors(self, data: dict) -> None:
        if data.get('errors'):
            logger.warning("SIMPLEFIN ERRORS DETECTED:")
            for error in data['errors']:
                logger.warning(error)
            if not self.ignore_failed:
                raise RuntimeError("Errors detected with registered Simplefin accounts. Please correct before proceeding!")
            else:
                logger.info("Config set to ignore errors, proceeding...")
    
    def retrieve_old_data(self, data_file) -> dict:
        if data_file.is_file():
            with open(data_file, 'r') as f:
                update_data = json.load(f)
        else:
            update_data = dict()
        if not update_data:
            update_data = self._set_up_new_data()
        return update_data
        
    def save_new_data(self, new_data, data_file) -> None:
        with open(data_file, 'w') as f:
            update_data = json.dump(new_data, f, indent=4)
        
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
    
    def update_txns_from_dicts(self, read_dict, current_dict, archive_dict) -> None:
        """
        takes in a simplefin JSON dict along with two other dictionaries representing current and archivable data 
        
        modifies the current and archivable data dicts based on the read_dict
        """
        for account in read_dict['accounts']:
            archive_account = self._get_account(archive_dict['accounts'], account)
            current_account = self._get_account(current_dict['accounts'], account)
            for txn in account['transactions']:
                if txn['transacted_at'] < self._archive_cutoff:
                    self.merge_new_txn(archive_account['transactions'], txn)
                else:
                    self.merge_new_txn(current_account['transactions'], txn)
            archive_account['transactions'].sort(key=self._sort_function)
            current_account['transactions'].sort(key=self._sort_function)
    
    def update_account_info(self, read_dict, current_dict) -> None:
        update_keys = ('balance', 'available-balance', 'balance-date')
        for read_account in read_dict['accounts']:
            for account in current_dict['accounts']:
                if self.account_matches(read_account, account):
                    for key in update_keys:
                        account[key] = read_account[key]
    
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
            
    
    def save_and_archive_data(self, data) -> None:
        # TODO: currently only transactions are supported--add support for holdings
        data_file = self.output_dir / DATA_FILE_NAME
        archive_file = self.output_dir / ARCHIVE_FILE_NAME
        
        file_data = self.retrieve_old_data(data_file)  # might contain archivable data
        archive_data = self.retrieve_old_data(archive_file)  # adjustments to archive_days may make some of this data "current"
        
        current_dict = self._set_up_new_data()
        archive_dict = self._set_up_new_data()
        
        logger.info(f"Loading archive data from {archive_file}...")
        self.update_txns_from_dicts(archive_data, current_dict, archive_dict)
        self.update_account_info(archive_data, archive_dict)
        self.update_account_info(archive_data, current_dict)
        
        logger.info(f"Loading existing data from {data_file}...")
        self.update_txns_from_dicts(file_data, current_dict, archive_dict)
        self.update_account_info(file_data, archive_dict)
        self.update_account_info(file_data, current_dict)
        
        logger.info("Ingesting feched data...")
        self.update_txns_from_dicts(data, current_dict, archive_dict)
        self.update_account_info(data, current_dict)
        self.update_account_info(data, archive_dict)
        
        current_dict['errors'] = data.get('errors')
        
        self._scrub_empty_accounts(current_dict)
        self._scrub_empty_accounts(archive_dict)
        
        logger.info(f"Saving data to {data_file}...")
        self.save_new_data(current_dict, data_file)
        logger.info(f"Saving data to {archive_file}...")
        self.save_new_data(archive_dict, archive_file)
    
    def run(self) -> None:
        if not self.verify_under_rate_limits():
            raise RuntimeError("Simplefin scraper aborted: maximum 20 calls per day.")
        
        new_data = self.fetch_data()
        self.update_rate_limits()
        
        self.log_errors(new_data)
        self.save_and_archive_data(new_data)
        

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


def run(**kwargs):
    scraper = SimplefinScraper(**kwargs)
    scraper.run()


@contextlib.contextmanager
def interactive(**kwargs):
    scraper = SimplefinScraper(**kwargs)
    kwargs['scraper'] = scraper
    yield kwargs