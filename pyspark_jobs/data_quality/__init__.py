"""Config-driven data quality framework.

``checks.py``    — individual check implementations (DataFrame -> CheckResult)
``framework.py`` — runs the declarative rules from configs/dq_rules.yaml
``run_dq.py``    — CLI entrypoint, writes results to the audit table
"""
