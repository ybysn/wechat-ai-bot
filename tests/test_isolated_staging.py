import importlib.util
import os
from pathlib import Path
import stat
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


_INSTALLER = Path(__file__).resolve().parents[1] / "deploy/isolated/install-staging.py"
_SPEC = importlib.util.spec_from_file_location("isolated_staging_installer", _INSTALLER)
installer = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(installer)


class StagingInstallerTests(unittest.TestCase):
    def test_configure_directory_sets_permissions_on_new_and_existing_paths(self):
        with tempfile.TemporaryDirectory() as temp:
            directory = Path(temp) / "stage"
            uid, gid = os.geteuid(), os.getegid()
            installer.configure_directory(directory, uid, gid, 0o755)
            installer.configure_directory(directory, uid, gid, 0o2770, created=True)
            details = directory.stat()
            self.assertEqual(stat.S_IMODE(details.st_mode), 0o2770)
            self.assertEqual((details.st_uid, details.st_gid), (uid, gid))

    def test_resume_refuses_nonempty_first_directory_before_installing(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            first = root / "code"
            first.mkdir(mode=0o700)
            original_lstat = Path.lstat

            def lstat(path, *args, **kwargs):
                if path == first:
                    return SimpleNamespace(st_uid=0, st_gid=0, st_mode=stat.S_IFDIR | 0o700)
                return original_lstat(path, *args, **kwargs)

            with patch.multiple(installer, CODE=first, CONFIG=root / "etc", DATA=root / "data", SYSTEMD=root), \
                 patch.object(installer, "check_installation_parents"), \
                 patch.object(installer, "verify_identities"), patch.object(Path, "lstat", lstat):
                installer.check_resume_checkpoint()
                (first / "existing").touch()
                with self.assertRaisesRegex(RuntimeError, "Unexpected interrupted directory"):
                    installer.check_resume_checkpoint()
