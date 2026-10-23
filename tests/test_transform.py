"""Offline tests for lib/transform.py. Run: python -B -m unittest discover -s tests"""

import io
import json
import os
import sys
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "lib"))
import transform  # noqa: E402

APP_MAP = {
    "string_rewrites": {"rules": [
        {"scope": None, "master": "/mnt/ssdmaster-pool/encrypted-ds/app-ds", "backup": "/mnt/backup-pool/encrypted-ds/app-ds"},
        {"scope": None, "master": "/mnt/master-pool/encrypted-ds", "backup": "/mnt/backup-pool/encrypted-ds"},
        {"scope": None, "master": "192.168.0.10", "backup": "192.168.0.60"},
        {"scope": "http", "master": "truenas-master", "backup": "truenas-backup"},
        {"scope": None, "master": "unconfigured", "backup": None},
    ]},
    "value_overrides": {"rules": [{"key": "certificate_id", "master": 2, "backup": 3}]},
    "keep_target": {"rules": [{"path": ["plex", "claim_token"], "default": ""}]},
}

VM_MAP = {
    "DISK": {"_doc": "x", "path": [
        {"scope": "/encrypted-ds/vm-ds", "master": "ssdmaster-pool/encrypted-ds/vm-ds", "backup": "backup-pool/encrypted-ds/vm-ds"},
    ]},
    "NIC": {"nic_attach": [{"scope": None, "master": "enp1s0f1", "backup": "ens6"}]},
    "DISPLAY": {
        "port": [{"scope": None, "master": 5900, "backup": 5950}],
        "web_port": [{"scope": None, "master": 5901, "backup": 5951}],
    },
}


def app(config, source="master", target="backup", target_config=None, name="immich"):
    return transform.transform_app(config, APP_MAP, source, target, name, target_config)


def vm(*attributes, source="master", target="backup"):
    doc = {"name": "v", "devices": [{"id": i, "attributes": a} for i, a in enumerate(attributes)]}
    report = transform.transform_vm(doc, VM_MAP, source, target)
    return [d["attributes"] for d in doc["devices"]], report


class AppDefinition(unittest.TestCase):
    def test_whole_token_only(self):
        out = app({"a": ["192.168.0.10", "192.168.0.100", "x192.168.0.10", "192.168.0.10:80,192.168.0.10"]})
        self.assertEqual(out["a"], ["192.168.0.60", "192.168.0.100", "x192.168.0.10", "192.168.0.60:80,192.168.0.60"])

    def test_paths_in_rule_order(self):
        out = app({"p": "/mnt/ssdmaster-pool/encrypted-ds/app-ds/x", "m": "/mnt/master-pool/encrypted-ds/media-ds"})
        self.assertEqual(out, {"p": "/mnt/backup-pool/encrypted-ds/app-ds/x", "m": "/mnt/backup-pool/encrypted-ds/media-ds"})

    def test_scope(self):
        out = app({"url": "http://truenas-master:3100", "hostname": "truenas-master"})
        self.assertEqual(out, {"url": "http://truenas-backup:3100", "hostname": "truenas-master"})

    def test_app_name_convention(self):
        self.assertEqual(app({"n": "ix-immich-master-server-1"})["n"], "ix-immich-backup-server-1")
        self.assertEqual(app({"n": "immich-backup"}, "backup", "master")["n"], "immich-master")

    def test_replacement_is_literal(self):
        with_backslash = {"s": "C:\\x 192.168.0.10 $1 \\1"}
        self.assertEqual(app(with_backslash)["s"], "C:\\x 192.168.0.60 $1 \\1")

    def test_keys_and_non_strings_untouched(self):
        out = app({"192.168.0.10": 192, "n": [None, True, 1.5]})
        self.assertEqual(out, {"192.168.0.10": 192, "n": [None, True, 1.5]})

    def test_unconfigured_rule_ignored(self):
        self.assertEqual(app({"s": "unconfigured"})["s"], "unconfigured")

    def test_value_override_json_equality(self):
        out = app({"a": {"certificate_id": 2}, "b": [{"certificate_id": 2.0}],
                   "c": {"certificate_id": True}, "d": {"certificate_id": "2"}})
        self.assertEqual(out, {"a": {"certificate_id": 3}, "b": [{"certificate_id": 3}],
                               "c": {"certificate_id": True}, "d": {"certificate_id": "2"}})

    def test_keep_target(self):
        source = {"plex": {"claim_token": "from-source"}}
        self.assertEqual(app(source, target_config={"plex": {"claim_token": "own"}}, name="plex"),
                         {"plex": {"claim_token": "own"}})
        self.assertEqual(app(source, name="plex"), {"plex": {"claim_token": ""}})
        self.assertEqual(app({"plex": {}}, target_config={"plex": {"claim_token": "own"}}, name="plex"),
                         {"plex": {}})


class VmDefinition(unittest.TestCase):
    def test_exact_match_keeps_type(self):
        (display,), report = vm({"dtype": "DISPLAY", "port": 5900, "web_port": 5901})
        self.assertEqual(display, {"dtype": "DISPLAY", "port": 5950, "web_port": 5951})
        self.assertEqual(report, [("DISPLAY", "port", "5900 → 5950"), ("DISPLAY", "web_port", "5901 → 5951")])

    def test_partial_match_in_scope(self):
        (disk, other), _ = vm({"dtype": "DISK", "path": "/dev/zvol/ssdmaster-pool/encrypted-ds/vm-ds/v/d0"},
                              {"dtype": "DISK", "path": "/dev/zvol/ssdmaster-pool/encrypted-ds/app-ds/x"})
        self.assertEqual(disk["path"], "/dev/zvol/backup-pool/encrypted-ds/vm-ds/v/d0")
        self.assertEqual(other["path"], "/dev/zvol/ssdmaster-pool/encrypted-ds/app-ds/x")

    def test_partial_match_becomes_string(self):
        (display,), _ = vm({"dtype": "DISPLAY", "port": 15900, "web_port": 5901})
        self.assertEqual(display["port"], "15950")

    def test_attributes_independent(self):
        (display,), _ = vm({"dtype": "DISPLAY", "port": 5900, "web_port": None})
        self.assertEqual(display, {"dtype": "DISPLAY", "port": 5950, "web_port": None})

    def test_reverse_direction_and_unmapped(self):
        (nic, pci), report = vm({"dtype": "NIC", "nic_attach": "ens6"}, {"dtype": "PCI", "pptdev": "ens6"},
                                source="backup", target="master")
        self.assertEqual((nic["nic_attach"], pci["pptdev"]), ("enp1s0f1", "ens6"))
        self.assertEqual(len(report), 1)

    def test_rejects_missing_devices(self):
        with self.assertRaises(ValueError):
            transform.transform_vm({"name": "v"}, VM_MAP, "master", "backup")


class Cli(unittest.TestCase):
    def run_cli(self, *argv):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            try:
                code = transform.main(list(argv))
            except SystemExit as e:
                code = e.code
        return code, out.getvalue(), err.getvalue()

    def test_app_definition_round_trip(self):
        with tempfile.TemporaryDirectory() as d:
            paths = {n: os.path.join(d, n + ".json") for n in ("map", "src", "tgt", "out")}
            for n, doc in (("map", APP_MAP), ("src", {"plex": {"claim_token": "s"}, "ip": "192.168.0.10"}),
                           ("tgt", {"plex": {"claim_token": "t"}})):
                with open(paths[n], "w", encoding="utf-8") as f:
                    json.dump(doc, f)
            code, _, _ = self.run_cli("app-definition", "--map", paths["map"], "--source", "master",
                                      "--target", "backup", "--app", "plex", "--target-config", paths["tgt"],
                                      paths["src"], paths["out"])
            self.assertEqual(code, 0)
            with open(paths["out"], encoding="utf-8") as f:
                self.assertEqual(json.load(f), {"plex": {"claim_token": "t"}, "ip": "192.168.0.60"})

    def test_bad_input_exits_1(self):
        with tempfile.TemporaryDirectory() as d:
            src = os.path.join(d, "src.json")
            with open(src, "w", encoding="utf-8") as f:
                f.write("{not json")
            code, _, err = self.run_cli("vm-definition", "--map", src, "--source", "master",
                                        "--target", "backup", src, os.path.join(d, "out.json"))
            self.assertEqual(code, 1)
            self.assertIn("vm-definition", err)

    def test_same_source_and_target_rejected(self):
        code, _, _ = self.run_cli("vm-definition", "--map", "m", "--source", "master", "--target", "master", "a", "b")
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
