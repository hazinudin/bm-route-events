from src.service.validation_result.result import ValidationResult
import unittest
import polars as pl
import cProfile
import pstats


class TestValidationResult(unittest.TestCase):
    def test_all_messages(self):
        """
        Test return all message as Arrow table or Pandas DataFrame.
        """
        result = ValidationResult('1234')
        msg_temp = 'Error msg {0}'

        for i in range(5):
            msg = msg_temp.format(i)
            result.add_message(msg, 'error', 'force')
            result.add_message(msg, 'rejected')

        self.assertTrue(type(result.get_all_messages()) == pl.DataFrame)
        self.assertTrue(result.status == 'rejected')
    
    def test_all_messages_empty(self):
        """
        Test if there is no message
        """
        result = ValidationResult('1234')

        self.assertTrue(result.get_all_messages().is_empty)
        self.assertTrue(result.status == 'verified')

    def test_to_smd_format(self):
        """
        Test SMD format output
        """
        result = ValidationResult('01001')
        result.add_message('error msg', 'error')
        result.add_message('error_sanggah msg', 'error', 'force')
        result.add_message('review msg', 'review')
        result.add_message('rejected msg', 'rejected')

        self.assertTrue(
            result.to_smd_format() == 
            {'status': 'Rejected', 'messages': ['rejected msg']}
        )

        result = ValidationResult('01001')
        result.add_message('error msg', 'error')
        result.add_message('error_sanggah msg', 'error', 'force')
        result.add_message('review msg', 'review')

        self.assertTrue(
            result.to_smd_format() == 
            {
                'status': 'Succeeded',
                'messages': [
                    {
                        'linkid': '01001',
                        'status': 'error',
                        'msg': 'error msg'
                    },
                    {
                        'linkid': '01001',
                        'status': 'error_sanggah',
                        'msg': 'error_sanggah msg'
                    }
                ]
            }
        )

    def test_to_smd_format_multiple_rejected_msg(self):
        """
        Test to_smd_format with multiple duplicated rejected messages.
        """
        result = ValidationResult('01001')
        result.add_message('error msg', 'rejected')
        result.add_message('error msg', 'rejected')

        self.assertTrue(
            result.to_smd_format() == 
            {'status': 'Rejected', 'messages': ['error msg']}
        )

    def test_to_smd_force_write(self):
        """
        Test to_smd_format with ignoring the 'force' message.
        """
        result = ValidationResult('01001', ignore_in='force')
        result.add_message('error force', 'error', 'force')
        result.add_message('error review', 'review', 'review')

        self.assertTrue(
            result.to_smd_format(show_all_msg=False)['messages'][0]['msg'] == 'error review'
        )

        self.assertTrue(
            len(result.to_smd_format(show_all_msg=False)['messages']) == 1
        )

        result = ValidationResult('01001', ignore_in='force')
        result.add_message('error msg', 'error')
        result.add_message('error force', 'error', 'force')
        result.add_message('error review', 'review', 'review')

        self.assertTrue(
            result.to_smd_format(show_all_msg=False)['messages'][0]['msg'] == 'error msg' 
        )

        self.assertTrue(
            len(result.to_smd_format(show_all_msg=False)['messages']) == 1
        )

    def test_validation_status(self):
        """
        Test validation status state.
        """

        result = ValidationResult('1234')

        self.assertTrue(result.status == 'verified')

        result.add_message("error message", 'error', None)
        self.assertTrue(result.status == 'error')

        result.add_message('review message', 'review', None)
        self.assertTrue(result.status == 'error')

    def test_to_invij_format(self):
        """
        Test INVI-J format output.
        """
        with cProfile.Profile() as profile:
            result = ValidationResult('1234')

            result.add_message('error msg', 'error')
            result.add_message('error msg 1', 'error')
            result.add_message('review msg', 'review')

            res = pstats.Stats(profile)
            res.sort_stats(pstats.SortKey.TIME)

        self.assertTrue(
            result.to_invij_format() == {
                "general":{
                    "status": "verified",
                    "error": []
                },
                "status": "failed",
                "error": ["error msg", "error msg 1"],
                "review": []
            }
        )

    def test_to_invij_format_ignored_status(self):
        """
        Test INVI-J format output.
        """
        result = ValidationResult('1234', ignore_in='review')

        result.add_message('review msg', 'review', 'review')
        result.add_message('review msg 1', 'review', 'review')
        result.add_message('error msg', 'error')

        self.assertTrue(
            result.to_invij_format() == {
                "general":{
                    "status": "verified",
                    "error": []
                },
                "status": "failed",
                "error": ['error msg'],
                "review": []
            }
        )

        result = ValidationResult('1234', ignore_in='force')

        result.add_message('review msg', 'review', 'review')
        result.add_message('error msg', 'error', 'force')

        self.assertTrue(
            result.to_invij_format() == {
                "general":{
                    "status": "verified",
                    "error": []
                },
                "status": "review",
                "error": [],
                "review": ["review msg"]
            }
        )

        result.add_message("rejected", 'rejected')

        self.assertTrue(
            result.to_invij_format() == {
                "general":{
                    "status": "failed",
                    "error": ["rejected"]
                },
                "status": "unverified",
                "error": [],
                "review": []
            }
        )



