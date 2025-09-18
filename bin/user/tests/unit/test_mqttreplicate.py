#    Copyright (c) 2025 Rich Bell <bellrichm@gmail.com>
#
#    See the file LICENSE.txt for your full rights.
#

import configobj
import logging

import unittest
import mock

import user.mqttreplicate

class TestConfiguration(unittest.TestCase):
    def test_enable_is_false(self):
        print("start")

        mock_engine = mock.Mock()
        config_dict = {
            'MQTTReplicate': {
                'Responder': {
                    'enable': False
                }
            }
        }
        config = configobj.ConfigObj(config_dict)
        logger = logging.getLogger('user.mqttreplicate')
        with mock.patch.object(logger, 'info') as mock_info:
            user.mqttreplicate.MQTTResponder(mock_engine, config)
            mock_info.assert_called_once_with(
                "Responder not enabled, exiting.")

        print("end")

if __name__ == '__main__':
    test_suite = unittest.TestSuite()                                                    # noqa: E265
    test_suite.addTest(TestConfiguration('test_enable_is_false'))  # noqa: E265
    unittest.TextTestRunner().run(test_suite)                                            # noqa: E265

    #unittest.main(exit=False)                                                           # noqa: E265
