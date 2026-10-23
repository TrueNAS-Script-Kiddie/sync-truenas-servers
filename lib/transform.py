#!/usr/bin/env python3
"""Source -> target transforms for replication: rewrites every per-host value.

Pure JSON in -> JSON out, stdlib only (runs on the hosts' system python3, 3.11), so every
transform is testable offline against saved JSON (tests/test_transform.py).

Subcommands:
  vm-definition   vm.query entry of one VM, rules from config/vm_device_mappings.json
  app-definition  app.config of one app, rules from config/app_config_mappings.json
                  (spec: config/app_divergences.md)

Errors go to stderr with exit 1; the caller turns that into Background_error.
"""

import argparse
import json
import re
import sys


def load_json(path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_json(path, doc):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(doc, f, indent=2, ensure_ascii=False)
        f.write("\n")


def json_equal(a, b):
    """JSON equality: true != 1 (Python's True == 1), 2 == 2.0."""
    if isinstance(a, bool) or isinstance(b, bool):
        return a is b
    if isinstance(a, (int, float)) and isinstance(b, (int, float)):
        return a == b
    if type(a) is not type(b):
        return False
    if isinstance(a, list):
        return len(a) == len(b) and all(map(json_equal, a, b))
    if isinstance(a, dict):
        return a.keys() == b.keys() and all(json_equal(a[k], b[k]) for k in a)
    return a == b


def to_text(value):
    """A JSON value as text, the way it reads in the config (5900, not 5900.0)."""
    if isinstance(value, str):
        return value
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def get_path(doc, path):
    """Value at path, or None when any step is missing or of the wrong type."""
    for step in path:
        if isinstance(doc, dict) and isinstance(step, str):
            doc = doc.get(step)
        elif isinstance(doc, list) and isinstance(step, int) and -len(doc) <= step < len(doc):
            doc = doc[step]
        else:
            return None
    return doc


def set_path(doc, path, value):
    """Set an existing path (callers check it with get_path first)."""
    for step in path[:-1]:
        doc = doc[step]
    doc[path[-1]] = value


def configured(rules, source, target):
    """Rules whose source and target side are both set; a null side means not yet configured."""
    return [r for r in rules if r.get(source) is not None and r.get(target) is not None]


###################
# VM definition   #
###################

def transform_vm(vm, mapping, source, target):
    """Rewrite device attributes per dtype; returns the report rows (dtype, attribute, change).

    Per attribute the first rule wins whose scope (if any) and source value both occur in the
    current value: an exact match takes the target value as-is (keeps numbers numbers), a
    partial match replaces the first occurrence inside the string.
    """
    if not isinstance(vm, dict) or not isinstance(vm.get("devices"), list):
        raise ValueError("VM definition has no 'devices' list")

    report = []
    for device in vm["devices"]:
        attributes = device.get("attributes") if isinstance(device, dict) else None
        if not isinstance(attributes, dict):
            continue
        dtype = attributes.get("dtype")
        for attribute, rules in mapping.get(dtype, {}).items() if isinstance(dtype, str) else ():
            if attribute == "_doc":
                continue
            old = attributes.get(attribute)
            old_text = "" if old is None or old is False else to_text(old)
            for rule in rules:
                scope = rule.get("scope")
                from_text = to_text(rule.get(source))
                if (scope is None or scope in old_text) and from_text in old_text:
                    if old_text == from_text:
                        new = rule.get(target)
                    else:
                        new = old_text.replace(from_text, to_text(rule.get(target)), 1)
                    attributes[attribute] = new
                    report.append((dtype, attribute, f"{to_text(old)} → {to_text(new)}"))
                    break

    for device in vm["devices"]:
        if get_path(device, ["attributes", "dtype"]) is None:
            raise ValueError(f"transformed device has no attributes.dtype: {device!r}")
    return report


def print_columns(rows, indent="  "):
    """Like column -t: columns aligned, two spaces apart."""
    if not rows:
        return
    widths = [max(len(row[i]) for row in rows) for i in range(len(rows[0]) - 1)]
    for row in rows:
        cells = [cell.ljust(width) for cell, width in zip(row, widths)] + [row[-1]]
        print(indent + "  ".join(cells))


def cmd_vm_definition(args):
    vm = load_json(args.source_json)
    report = transform_vm(vm, load_json(args.map), args.source, args.target)
    write_json(args.out_json, vm)
    print_columns(report)


###################
# App definition  #
###################

def transform_app(config, mapping, source, target, app, target_config):
    """Rewrite host references anywhere in an app.config.

    - string_rewrites: literal, whole-token (192.168.0.10 leaves 192.168.0.100 alone), applied
      in order to every string value. The <app>-<source> -> <app>-<target> name rewrite comes
      first: it is the naming convention (Control_app_with_checks), not configuration.
    - value_overrides: an object key whose value equals the source side gets the target side.
    - keep_target: per-instance values (Plex claim token) come from the target's own config, or
      the rule's default when the target has none; only for paths the source has.
    """
    rewrites = [{"scope": None, source: f"{app}-{source}", target: f"{app}-{target}"}]
    rewrites += mapping.get("string_rewrites", {}).get("rules", [])
    rewrites = [
        (r.get("scope"),
         to_text(r[source]),
         to_text(r[target]),
         re.compile(r"(?<![A-Za-z0-9])" + re.escape(to_text(r[source])) + r"(?![A-Za-z0-9])"))
        for r in configured(rewrites, source, target)
    ]
    overrides = configured(mapping.get("value_overrides", {}).get("rules", []), source, target)

    def rewrite(text):
        for scope, from_text, to, pattern in rewrites:
            if (scope is None or scope in text) and from_text in text:
                text = pattern.sub(lambda _: to, text)
        return text

    # Bottom-up: an object's overrides see its already-rewritten children.
    def walk(value):
        if isinstance(value, dict):
            value = {key: walk(child) for key, child in value.items()}
            for rule in overrides:
                if rule["key"] in value and json_equal(value[rule["key"]], rule[source]):
                    value[rule["key"]] = rule[target]
            return value
        if isinstance(value, list):
            return [walk(child) for child in value]
        if isinstance(value, str):
            return rewrite(value)
        return value

    config = walk(config)
    for rule in mapping.get("keep_target", {}).get("rules", []):
        if get_path(config, rule["path"]) is None:
            continue
        kept = get_path(target_config, rule["path"])
        set_path(config, rule["path"], rule.get("default") if kept is None else kept)
    return config


def cmd_app_definition(args):
    target_config = load_json(args.target_config) if args.target_config else None
    config = transform_app(load_json(args.source_json), load_json(args.map),
                           args.source, args.target, args.app, target_config)
    write_json(args.out_json, config)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def add_common(p):
        p.add_argument("--map", required=True, help="mapping rules JSON")
        p.add_argument("--source", required=True, choices=["master", "backup"], help="source server id")
        p.add_argument("--target", required=True, choices=["master", "backup"], help="target server id")
        p.add_argument("source_json")
        p.add_argument("out_json")

    p = sub.add_parser("vm-definition", help="transform one VM's vm.query entry")
    add_common(p)
    p.set_defaults(func=cmd_vm_definition)

    p = sub.add_parser("app-definition", help="transform one app's app.config")
    add_common(p)
    p.add_argument("--app", required=True, help="bare app name (immich, not immich-master)")
    p.add_argument("--target-config", help="target's current app.config; absent when not installed")
    p.set_defaults(func=cmd_app_definition)

    args = parser.parse_args(argv)
    if args.source == args.target:
        parser.error("--source and --target must differ")
    try:
        args.func(args)
    except (OSError, ValueError, KeyError, TypeError) as e:
        print(f"transform.py {args.command}: {type(e).__name__}: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    # The report holds '→'; a non-UTF-8 locale must not turn that into an error
    sys.stdout.reconfigure(encoding="utf-8")
    sys.exit(main())
