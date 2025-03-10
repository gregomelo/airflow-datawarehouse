"""
CoinGecko Coins List Bronze Transformer.

This module defines a transformer that processes CoinGecko's coins list
from the bronze layer to the silver layer in the data pipeline.
"""

from pandera import DataFrameModel

from include.contracts.coingecko.coins_list import CoinGeckoCoinsListSchema
from include.transformers.coingecko.coingecko_base import CoinGeckoTranformerBase


class CoinGeckoCoinsListBronze(CoinGeckoTranformerBase):
    """
    Transformer for CoinGecko's coins list from the bronze to the silver layer.

    This class implements a transformation step in the ETL pipeline,
    processing CoinGecko coins list data stored in the bronze layer and
    preparing it for the silver layer.

    Attributes
    ----------
    source_surname : str
        The specific source within CoinGecko, set as "coins_list".
    layer_from : str
        The origin layer of the data, set as "bronze".
    layer_to : str
        The destination layer of the data, set as "silver".
    data_contract : DataFrameModel
        The schema used for data validation, based on `CoinGeckoCoinsListSchema`.
    """

    source_surname: str = "coins_list"
    layer_from: str = "bronze"
    layer_to: str = "silver"
    data_contract: DataFrameModel = CoinGeckoCoinsListSchema

    def _get_query(self) -> str:
        """
        Generate the SQL query to transform and load CoinGecko coins list data.

        This query reads JSON files from the bronze layer, extracts necessary fields,
        unnests the platform mapping, and converts filenames into timestamp fields
        for the silver layer.

        Returns
        -------
        str
            The SQL query string used for data transformation.
        """
        load_path: str = f"{str(self.load_from)}/*.json"

        sql_query: str = (
            f"""
            SELECT
                id,
                symbol,
                name,
                COALESCE(platform_data.unnest.key, '') AS platform_name,
                COALESCE(platform_data.unnest.value, '') AS contract_address,
                strptime(
                    left(right(filename, 26),17), '%d-%m-%yT%H_%M_%S'
                ) AS extracted_at
            FROM read_json(
                    '{load_path}',
                    filename=true,
                    auto_detect=false,
                    columns= {
                        {
                            'id': 'VARCHAR',
                            'symbol': 'VARCHAR',
                            'name': 'VARCHAR',
                            'platforms': 'MAP(VARCHAR, VARCHAR)'
                        }
                    }
                    )
            LEFT JOIN UNNEST(map_entries(platforms)) AS platform_data ON TRUE;
            """  # nosec
        )

        return sql_query
