import os
import sys
import json
import pandas as pd
import unittest
from unittest.mock import MagicMock, patch


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.etl import ETL  # type: ignore

mock_response = {
                "@odata.context": "http://test_gho/api",
                "@odata.count": 194,
                "value": [
                    {
                        "Id": 11044099,
                        "IndicatorCode": "NCD_CCS_BreastCancer",
                        "SpatialDimType": "COUNTRY",
                        "SpatialDim": "AFG",
                        "TimeDimType": "YEAR",
                        "ParentLocationCode": "EMR",
                        "ParentLocation": "Eastern Mediterranean",
                        "Dim1Type": None,
                        "TimeDim": 2013,
                        "Dim1": None,
                        "Dim2Type": None,
                        "Dim2": None,
                        "Dim3Type": None,
                        "Dim3": None,
                        "DataSourceDimType": None,
                        "DataSourceDim": None,
                        "Value": "Yes",
                        "NumericValue": None,
                        "Low": None,
                        "High": None,
                        "Comments": None,
                        "Date": "2015-06-01T13:06:16.897+02:00",
                        "TimeDimensionValue": "2013",
                        "TimeDimensionBegin": "2013-01-01T00:00:00+01:00",
                        "TimeDimensionEnd": "2013-12-31T00:00:00+01:00"
                    },
                    {
                        "Id": 11044101,
                        "IndicatorCode": "NCD_CCS_BreastCancer",
                        "SpatialDimType": "COUNTRY",
                        "SpatialDim": "AGO",
                        "TimeDimType": "YEAR",
                        "ParentLocationCode": "AFR",
                        "ParentLocation": "Africa",
                        "Dim1Type": None,
                        "TimeDim": 2013,
                        "Dim1": None,
                        "Dim2Type": None,
                        "Dim2": None,
                        "Dim3Type": None,
                        "Dim3": None,
                        "DataSourceDimType": None,
                        "DataSourceDim": None,
                        "Value": "No data received",
                        "NumericValue": None,
                        "Low": None,
                        "High": None,
                        "Comments": None,
                        "Date": "2015-06-01T13:06:16.95+02:00",
                        "TimeDimensionValue": "2013",
                        "TimeDimensionBegin": "2013-01-01T00:00:00+01:00",
                        "TimeDimensionEnd": "2013-12-31T00:00:00+01:00"
                    }
                ]
            }


class TestETL(unittest.TestCase):

    def setUp(self):
        self.config = {
                "api": "http://test_gho/api",
                "destination": {
                    "postgres": {
                        "url": "postgresql://postgres:test_db@localhost:5432/postgres"
                                }
                        },
                "indicator": "NCD_CCS_BreastCancer",
                "transform": {
                    "columns": {
                        "column1": "new_column1",
                        "column2": "new_column2",
                            }
                        }
                    }
        self.logger = MagicMock()

    def test_construct_api_url(self):
        etl = ETL(self.config, "NCD_CCS_BreastCancer", self.logger)
        api_url = etl.construct_api_url(10, 0)
        expected_url = "http://test_gho/api/NCD_CCS_BreastCancer?$count=true&$top=10&$skip=0"
        self.assertEqual(api_url, expected_url)

    def test_extract(self):
        with patch('requests.post') as mock_post:
            mock_post.return_value.text = json.dumps(mock_response)
            etl = ETL(self.config, "NCD_CCS_BreastCancer", self.logger)
            result = etl.extract(2, 0)

        # Assert value on the data frame
        self.assertEqual(result.iloc[0]["Value"], "Yes")
        self.assertEqual(result.iloc[1]["Value"], "No data received")

        # Assert SpatialDim values
        self.assertEqual(result.iloc[0]["SpatialDim"], "AFG")
        self.assertEqual(result.iloc[1]["SpatialDim"], "AGO")

    def test_transform(self):
        etl = ETL(self.config, "NCD_CCS_BreastCancer", self.logger)
        input_df = pd.DataFrame({"column1": [1, 2], "column2": [3, 4]})
        result = etl.transform(input_df)

        # Ensure that the transform method correctly renames columns
        self.assertListEqual(list(result.columns), ["new_column1", "new_column2"])


if __name__ == '__main__':
    unittest.main()
