from typing import Any, Mapping, Optional, Iterable, List, Tuple
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import SyncMode, ConnectorSpecification
from airbyte_cdk.sources.streams.http import HttpStream
from datetime import datetime, timedelta
import requests
import time

class KrakenLedgerStream(HttpStream):
    url_base = "https://treasury-stage.thein1.com/"

    def __init__(self, bearer_token: str):
        super().__init__()
        self.bearer_token = bearer_token
        self.start_date = datetime.now() - timedelta(days=10)

    @property
    def request_method(self) -> str:
        return "POST"

    def path(self, **kwargs) -> str:
        return "internal/integration/v1/dwh/kraken/ledgers"

    @property
    def primary_key(self) -> str:
        return "id"

    def next_page_token(self, response: requests.Response, offset: int = 0) -> Optional[Mapping[str, Any]]:
        json_data = response.json()
        ledger = json_data.get("result", {}).get("ledger", [])
        entries_count = json_data.get('result', {}).get('count', 0)  # Total records available

        # Check if we have reached or surpassed the total count
        if offset >= entries_count:
            self.logger.debug("Reached maximum records; stopping pagination.")
            return None

        # Increment the offset by 50 for the next page, if more records are available
        if len(ledger) == 50:
            next_offset = offset + 50
            next_token = {"ofs": next_offset}
            self.logger.debug(f"Next page token: {next_token}")
            return next_token

        # No more pages if fewer than 50 records returned in this page
        self.logger.debug("Fewer than 50 records; pagination complete.")
        return None


    def request_headers(self, **kwargs) -> Mapping[str, str]:
        return {
            'Authorization': f'Bearer {self.bearer_token}',
            'Content-Type': 'application/json'
        }

    def request_body_json(self, stream_state: Mapping[str, Any], next_page_token: Optional[Mapping[str, Any]], **kwargs) -> Mapping:
        end_timestamp = int(datetime.now().timestamp())
        start_timestamp = int(self.start_date.timestamp())
        offset = next_page_token.get("ofs", 0) if next_page_token else 0
        return {
            "aclass": "currency",
            "type": "all",
            "start": start_timestamp,
            "end": end_timestamp,
            "ofs": offset
        }

    def parse_response(self, response: requests.Response) -> Iterable[Mapping]:
        json_response = response.json()
        self.logger.info(f"Parsing response: {json_response}")
        ledger_entries = json_response.get("result", {}).get("ledger", {})

        if not ledger_entries:
            self.logger.warning("No ledger entries found in the response.")

        for key, entry in ledger_entries.items():
            yield {
                "id": key,
                "aclass": entry.get("aclass", ""),
                "amount": float(entry.get("amount", 0)),
                "asset": entry.get("asset", ""),
                "balance": float(entry.get("balance", 0)),
                "fee": float(entry.get("fee", 0)),
                "refid": entry.get("refid", ""),
                "time": datetime.fromtimestamp(entry.get("time", 0)).isoformat(),
                "type": entry.get("type", ""),
                "subtype": entry.get("subtype", ""),
            }

    def read_records(self, sync_mode: SyncMode, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Mapping[str, Any]]:
        offset = 0
        backoff_delay = 30  # Start with a 30-second delay
        max_backoff_delay = 120  # Cap at 2 minutes
        retries = 0  # Track number of retries for rate limit errors
        max_retries = 10  # Limit to 10 retries to avoid excessive requests

        while True:
            try:
                url = f"{self.url_base}{self.path()}"
                response = requests.post(
                    url,
                    headers=self.request_headers(),
                    json=self.request_body_json(stream_state, {"ofs": offset}),
                    timeout=120
                )
                response.raise_for_status()

                # Parse response
                json_data = response.json()
                if json_data.get("error") == ['EAPI:Rate limit exceeded']:
                    raise requests.exceptions.HTTPError("Rate limit exceeded")

                yield from self.parse_response(response)

                # Check for next page
                next_page = self.next_page_token(response)
                if not next_page:
                    break  # No more pages
                offset = next_page["ofs"]

                # Reset backoff after a successful request
                backoff_delay = 30
                retries = 0

            except requests.exceptions.HTTPError as e:
                # Handle rate limit error with backoff and retry limit
                if "Rate limit exceeded" in str(e):
                    retries += 1
                    if retries >= max_retries:
                        self.logger.error("Max retries for rate limit exceeded. Halting.")
                        break
                    self.logger.warning(f"Rate limit exceeded. Retrying in {backoff_delay} seconds...")
                    time.sleep(backoff_delay)
                    backoff_delay = min(backoff_delay * 1.5, max_backoff_delay)
                else:
                    self.logger.error(f"HTTP error: {e}")
                    break
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                break

class SourceKrakenLedger(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        try:
            stream = KrakenLedgerStream(config["bearer_token"])
            url = f"{stream.url_base}{stream.path()}"
            logger.info(f"Connecting to URL: {url}")
            response = requests.post(url, headers=stream.request_headers(), json=stream.request_body_json({}, None))
            response.raise_for_status()
            return True, None
        except Exception as e:
            return False, f"Failed to connect: {e}"

    def streams(self, config: Mapping[str, Any]) -> List[HttpStream]:
        return [KrakenLedgerStream(config["bearer_token"])]

    def spec(self, logger) -> ConnectorSpecification:
        return ConnectorSpecification(
            documentationUrl="https://docs.airbyte.io/integrations/sources/kraken",
            connectionSpecification={
                "type": "object",
                "properties": {
                    "bearer_token": {
                      "type": "string",
                      "description": "Bearer token for API authentication"
                    }
                },
                "required": ["bearer_token"]
            }
        )
