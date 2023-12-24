from unittest.mock import patch

from tests.Utils import *
from commons.backtest.fastBT import FastBT
from commons.loggers.setup_logger import setup_logging


class TestFastBT(unittest.TestCase):
    fb = FastBT()
    scrip = "NSE_ACME"
    strategy = "TEST.ME"
    count = 100

    @staticmethod
    def __format_df(df) -> pd.DataFrame:
        output_df = df.copy()
        return output_df.astype(object)

    @patch('commons.dataprovider.ScripData.ScripData')
    def test_prep_data(self, mock_api):
        tick_data = read_file_df(name="fastBT/tick-data.csv")
        base_data = read_file_df(name="fastBT/base-data.csv")
        pred_data = read_file_df(name="fastBT/raw-pred-df.csv")

        mock_api.get_tick_data.return_value = tick_data
        mock_api.get_base_data.return_value = base_data

        ret_val = self.fb.prep_data(self.scrip, strategy=self.strategy, raw_pred_df=pred_data, sd=mock_api)
        actual_bod = ret_val.loc[pd.notnull(ret_val.signal)].reset_index(drop=True)
        actual_bod['date'] = actual_bod['date'].astype(str)
        actual_bod['datetime'] = actual_bod['datetime'].astype(str)
        actual_cob = ret_val.loc[ret_val.cob_row == 1.0].reset_index(drop=True)
        actual_cob['date'] = actual_cob['date'].astype(str)
        actual_cob['datetime'] = actual_cob['datetime'].astype(str)

        expected_bod_df = read_file_df(name="fastBT/expected-bod-df.csv")
        expected_cob_df = read_file_df(name="fastBT/expected-cob-df.csv")

        self.assertEqual(2999, len(ret_val))
        pd.testing.assert_frame_equal(expected_bod_df, actual_bod)
        pd.testing.assert_frame_equal(expected_cob_df, actual_cob)


if __name__ == "__main__":
    setup_logging("test_fastBT.log")
    unittest.main()
