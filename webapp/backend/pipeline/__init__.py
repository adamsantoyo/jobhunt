"""Explicit canonical pipeline utilities."""


def build_audit_report(conn):
	from .audit import build_audit_report as build

	return build(conn)


__all__ = ["build_audit_report"]
