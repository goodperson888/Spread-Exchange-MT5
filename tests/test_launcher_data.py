import io
import json
import unittest
from unittest.mock import patch

import _test_runtime
import launcher
import server


class LauncherDataTests(unittest.TestCase):
    def start_existing(self, data_directory):
        status={'app':'GoldPairLocal'}
        if data_directory is not None: status['data_directory']=data_directory
        response=io.BytesIO(json.dumps(status).encode())
        with patch.object(server,'stop_legacy_paper_on_boot'), \
             patch.object(server,'ThreadingHTTPServer',side_effect=OSError('port occupied')), \
             patch('launcher.sys.argv',['GoldPairLocal.exe']), \
             patch('launcher.urllib.request.urlopen',return_value=response), \
             patch('launcher.webbrowser.open') as browser,patch('builtins.print'):
            launcher.main()
        return browser

    def test_same_data_can_open_existing_application(self):
        self.start_existing(str(server.DATA.resolve())).assert_called_once()

    def test_migration_does_not_silently_reuse_old_process(self):
        self.start_existing(str(server.DATA.resolve())+'-different').assert_not_called()
        self.start_existing(None).assert_not_called()
