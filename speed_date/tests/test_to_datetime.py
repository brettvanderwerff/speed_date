import unittest
from speed_date.src.to_datetime import to_datetime
import os
import pandas as pd


class TestToDatetime(unittest.TestCase):

    def test_to_datetime(self):
        '''
        Tests that datetime conversion does not affect the shape of the object (i.e. column, row row number remains the
        same. And tests that the object is changed from type "object" to type "datetime".
        '''
        df = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                      os.path.basename('test_file.txt')),
                         sep='\t', skiprows=27, header=None,
                         names=['agency', 'site_id', 'datetime', 'time_zone', 'discharge', 'confidence'])

        test_df = df.copy()
        test_df['datetime'] = to_datetime(test_df['datetime'])

        self.assertEqual(df.shape, test_df.shape)
        self.assertTrue(test_df['datetime'].dtype == 'datetime64[ns]')


if __name__ == "__main__":
    unittest.main()
