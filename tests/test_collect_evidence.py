"""Offline tests for tools/collect_evidence.py: masking, token scan, SQLite dump, leak check."""
import argparse
import json
import os
import sqlite3
import sys
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "tools"))
import collect_evidence as ce  # noqa: E402

ARGS = argparse.Namespace(workload=["x"], max_config_rows=200, max_hit_lines=20)


def collector(sid="master", key="test-key-1"):
    c = ce.Collector(key, sid, ARGS)
    c.chase = True
    return c


class Masking(unittest.TestCase):
    def test_same_value_same_tag_across_hosts_and_key_changes_it(self):
        tag = collector("master").mask("hunter2-hunter2")
        self.assertEqual(tag, collector("backup").mask("hunter2-hunter2"))
        self.assertNotEqual(tag, collector(key="other-key-2").mask("hunter2-hunter2"))
        self.assertNotIn("hunter2", tag)

    def test_json_masks_by_key_name_env_entry_and_name_value_pair(self):
        c = collector()
        out = c.mask_json({
            "db_password": "s3cret-value-1", "port": 30037, "claim_token": "",
            "Env": ["GF_SECURITY_ADMIN_PASSWORD=adminpass-123", "TZ=Europe/Brussels"],
            "envs": [{"name": "API_TOKEN", "value": "tok-abcdefgh"}, {"name": "TZ", "value": "UTC"}],
            "ix_certificates": {"2": {"privatekey": "PEMPEMPEMPEM", "name": "truenas-master"}}})
        text = json.dumps(out)
        for secret in ("s3cret-value-1", "adminpass-123", "tok-abcdefgh", "PEMPEMPEMPEM"):
            self.assertNotIn(secret, text)
        self.assertEqual(out["port"], 30037)
        self.assertEqual(out["claim_token"], "")
        self.assertIn("TZ=Europe/Brussels", out["Env"])
        self.assertEqual(out["envs"][1]["value"], "UTC")
        self.assertEqual(out["ix_certificates"]["2"]["name"], "truenas-master")

    def test_text_masks_secret_lines_and_url_credentials_keeps_plain_values(self):
        c = collector()
        out = c.mask_text("admin_password = hunter2-hunter2\nsecret_key: \"abc-def-ghi\"\n"
                          "cookie_secure = false\nurl = postgres://u:pw-123456@db:5432/x\n"
                          "http_port = 3000\n")
        self.assertNotIn("hunter2", out)
        self.assertNotIn("abc-def-ghi", out)
        self.assertNotIn("pw-123456", out)
        self.assertIn("cookie_secure = false", out)
        self.assertIn("http_port = 3000", out)

    def test_final_mask_chases_a_known_secret_and_private_keys_everywhere(self):
        c = collector()
        c.mask("hunter2-hunter2")
        c.mask("datasource")                    # a plain word is masked in place, never chased
        c.mask("/grafana/certs/tls.key")        # so is a path
        c.mask("plugin-sample-value", chase=False)
        self.assertEqual(c.secrets, {"hunter2-hunter2"})
        pem = "-----BEGIN RSA PRIVATE KEY-----\nAAAA\n-----END RSA PRIVATE KEY-----"
        out = c.final_mask(f"x: hunter2-hunter2\n{pem}\n")
        self.assertNotIn("hunter2", out)
        self.assertNotIn("AAAA", out)
        self.assertEqual(c.leak_check({"f": out}), [])
        self.assertTrue(c.leak_check({"f": "aHVudGVyMi1odW50ZXIy"}))   # base64 of the secret

    def test_unpaired_private_key_marker_is_masked_with_what_follows(self):
        c = collector()
        out = c.final_mask('{"placeholder": "-----BEGIN PRIVATE KEY-----\\nMIIabc123\\n", "next": 1}')
        self.assertNotIn("PRIVATE KEY", out)
        self.assertNotIn("MIIabc123", out)
        self.assertIn('"next": 1', out)
        self.assertEqual(c.leak_check({"f": out}), [])


class TokenScan(unittest.TestCase):
    def setUp(self):
        self.c = collector("backup")

    def tokens(self, text):
        return [self.c.classify(t) for t in self.c.token_re.findall(text)]

    def test_tokens_are_classified_own_or_peer_longest_first(self):
        self.assertEqual(self.tokens("http://192.168.0.10:3100"), ["peer:192.168.0.10"])
        self.assertEqual(self.tokens("/mnt/ssdmaster-pool/x"), ["peer:ssdmaster-pool"])
        self.assertEqual(self.tokens("/mnt/master-pool/x"), ["peer:master-pool"])
        self.assertEqual(self.tokens("truenas-master"), ["peer:truenas-master"])
        self.assertEqual(self.tokens("loki-backup"), ["own:-backup"])

    def test_lookalikes_do_not_match(self):
        for text in ("192.168.0.100", "192.168.0.101:80", "1192.168.0.10", "-master", "x-masterful",
                     "10.192.168.0.10"):
            self.assertEqual(self.tokens(text), [], text)

    def test_definition_scan_reports_the_json_path(self):
        hits = self.c.scan_json({"network": {"root_url": "https://192.168.0.60:30037"},
                                 "storage": [{"path": "/mnt/backup-pool/a"}]})
        self.assertEqual([h["path"] for h in hits], ["$.network.root_url", "$.storage[0].path"])
        self.assertEqual(hits[0]["tokens"], ["own:192.168.0.60"])


class Sqlite(unittest.TestCase):
    def test_dump_has_schema_hits_and_masked_rows(self):
        db = sqlite3.connect(":memory:")
        db.executescript("""
            CREATE TABLE data_source (id INTEGER, url TEXT, secure_json_data TEXT, json_data TEXT);
            INSERT INTO data_source VALUES (1, 'http://192.168.0.10:3100', 'enc-secret-blob', '{"apiToken": "tok-abcdefgh"}');
            CREATE TABLE kv_store (id INTEGER, key TEXT, value TEXT);
            INSERT INTO kv_store VALUES (1, 'last_update', 'x');
            CREATE TABLE api_key (id INTEGER, name TEXT, key TEXT);
            INSERT INTO api_key VALUES (7, 'ci', 'keykeykeykey');
        """)
        c = collector("master")
        c.dump_sqlite("db", db)
        hits = json.loads(c.out["db/token-hits.json"])
        self.assertEqual([(h["table"], h["column"], h["token"], h["hits"]) for h in hits],
                         [("data_source", "url", "own:192.168.0.10", 1)])
        self.assertIn("CREATE TABLE data_source", c.out["db/schema.sql"])
        self.assertIn("api_key\t1", c.out["db/tables.tsv"])
        rows = c.out["db/rows/data_source.jsonl"] + c.out["db/rows/api_key.jsonl"]
        for secret in ("enc-secret-blob", "tok-abcdefgh", "keykeykeykey"):
            self.assertNotIn(secret, rows)
        self.assertIn("http://192.168.0.10:3100", rows)
        self.assertEqual(json.loads(c.out["db/rows/api_key.jsonl"])["id"], 7)
        self.assertEqual(json.loads(c.out["db/rows/kv_store.jsonl"])["key"], "last_update")

    def test_single_line_json_file_is_masked_per_key_not_as_one_line(self):
        c = collector()
        out = json.loads(c.mask_content("docker-compose.yaml",
                                        '{"configs": {"private": {"content": "KEYKEYKEY"}}, "services": {"g": {"image": "grafana"}}}'))
        self.assertEqual(out["services"]["g"]["image"], "grafana")
        self.assertNotIn("KEYKEYKEY", json.dumps(out))


if __name__ == "__main__":
    unittest.main()
