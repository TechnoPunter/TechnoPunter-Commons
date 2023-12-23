from tests.Utils import *
from commons.utils.Misc import remove_outliers


def test_remove_outliers():
    data = read_file_df("misc/weight-height.csv")
    result = remove_outliers(data['Height'], lower_cutoff=25, higher_cutoff=75)
    assert 9992, len(result)
