import unittest
from types import SimpleNamespace as NS
from mt5_session import binding_info, binding_error, initialize_options


class SessionBindingTests(unittest.TestCase):
    def test_windows_path_comparison_ignores_quotes_case_and_slashes(self):
        settings=dict(require_terminal_path=True,terminal_path=' "D:\\MT5-3\\terminal64.exe" ')
        info=binding_info(settings,NS(path='d:/mt5-3/',data_path='C:/Profiles/third'))
        self.assertTrue(info['path_matches']);self.assertEqual(binding_error(settings,info),'')
        self.assertEqual(initialize_options(settings),dict(timeout=10000))

    def test_portable_requires_its_actual_data_directory_even_if_program_matches(self):
        settings=dict(require_terminal_path=True,terminal_path=r'D:\MT5-3\terminal64.exe',portable=True)
        info=binding_info(settings,NS(path=r'D:\MT5-3',data_path=r'C:\old-profile'))
        self.assertIn('实际数据目录',binding_error(settings,info))
        info=binding_info(settings,NS(path=r'D:\MT5-3',data_path='d:/mt5-3/'))
        self.assertEqual(binding_error(settings,info),'')

    def test_unverifiable_pool_directory_is_rejected_and_legacy_auto_discovery_is_unchanged(self):
        settings=dict(require_terminal_path=True,terminal_path=r'D:\MT5-3\terminal64.exe')
        info=binding_info(settings,NS(connected=True))
        self.assertIn('无法核验',binding_error(settings,info))
        self.assertEqual(binding_error({},binding_info({},None)),'')
