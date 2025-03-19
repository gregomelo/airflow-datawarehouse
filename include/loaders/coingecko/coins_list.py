"""
CoinGecko Coins List Loaders.

This module defines a loader that processes CoinGecko's coins list
from the bronze layer to the silver layer in the data pipeline.
"""

import duckdb

from include.loaders.coingecko.coingecko_base import (
    CoinGeckoBaseGoldLoader,
    CoinGeckoBaseSilverLoader,
)
from include.utils.file_tools import storage_path


class CoinGeckoBaseCoinsListSilverLoader(CoinGeckoBaseSilverLoader):
    """
    Loader for transforming CoinGecko Coins List data from Bronze to Silver layer.

    This class extends `CoinGeckoBaseSilverLoader` and is specialized for processing
    the "coins_list" dataset from CoinGecko.

    Attributes
    ----------
    source_sourname : str
        The specific dataset identifier, set to "coins_list".
    """

    source_surname: str = "coins_list"


class CoinGeckoBaseCoinsListGoldLoader(CoinGeckoBaseGoldLoader):
    """
    Loader for transforming CoinGecko Coins List data from Bronze to Silver layer.

    This class extends `CoinGeckoBaseSilverLoader` and is specialized for processing
    the "coins_list" dataset from CoinGecko.

    Attributes
    ----------
    source_surname : str
        The specific dataset identifier, set to "coins_list".
    """

    source_surname: str = "coins_list"

    def _get_sql_gold(self):
        self._file_silver: str = (
            f"az://{self._container}/"
            f"{storage_path('silver', self.source_name, None)}/"
            f"{self._file_name_silver}"
        )

        self._sql_gold = f"""
            SELECT
                id,
                symbol,
                name,
                platform_name,
                contract_address,
                extracted_at
                FROM (
                    SELECT
                        id,
                        symbol,
                        name,
                        platform_name,
                        contract_address,
                        extracted_at,
                        RANK() OVER (PARTITION BY id ORDER BY extracted_at DESC) as row_number
                    FROM read_parquet('{self._file_silver}')
                    )
            WHERE row_number = 1
            """  # nosec

    def _check_data_quality(self, df_gold):
        sql_gold_quality = """
            SELECT
                id,
                COUNT ( DISTINCT ( extracted_at ) ) as rows
            FROM
                (SELECT
                    id,
                    symbol,
                    name,
                    platform_name,
                    contract_address,
                    extracted_at
                    FROM (
                        SELECT
                            id,
                            symbol,
                            name,
                            platform_name,
                            contract_address,
                            extracted_at,
                            RANK() OVER (
                                PARTITION BY id ORDER BY extracted_at DESC
                            ) as row_number
                        FROM df_gold
                        )
                WHERE row_number = 1
                )
            GROUP BY id
            HAVING rows > 1
            """  # nosec

        df_quality_check = duckdb.sql(sql_gold_quality)

        rows_garbage = len(df_quality_check.fetchall())

        if rows_garbage != 0:
            raise ValueError("Data quality check failed.")
