import pandas as pd

# NOTE: Generate Presigned URL
url = """
    ### PASTE PRESIGNED URL HERE ###  
"""

url = 'https://mybucket-7012.s3.us-east-1.amazonaws.com/bank-customer-churn-prediction.csv?response-content-disposition=inline&X-Amz-Security-Token=IQoJb3JpZ2luX2VjELv%2F%2F%2F%2F%2F%2F%2F%2F%2F%2FwEaDmFwLXNvdXRoZWFzdC0xIkgwRgIhAN5HtsRIMgvPdPxvYmLqGj8enkWlshdT4JOPE22RRBIaAiEAk8OYBVNqjH7wPR4ANU61QJWhJwsGmOp3q4s8glmVlZUq5AIIVBAAGgw1MDQxMDU3MjkzMDgiDLUcVZ1u4UGdEQKs4SrBApiHFztozpgpm0QbaUbS2DEFsRyytaUW4TQfK0F9kmgrBnCAc0qb46Nm5rCXoObXNo1i1SYw7gjl3nQD6MEGN9PqNX6N1Gq86aGoedaAM%2Fg7XReF%2FoBAlX16oHQKustNA0yHncnnvblBV9pF44mB8XEWbAsAiRvlxe7npKXIqEs32rP076YEumiFldoSOrL0koL8q13bVM17uHwRikqxKsxfuW3fsSzZW04QfBi0gm%2BF5KjBfrKrpb3lsRbdD%2FOXtW1Gjx5UIWBh8fER9GCJaMImPR5%2Bu4Ri2zYskWhO%2BbHP6V0uEdtBfn8dJSPUSHpnUOJKMQUoUK7faZGDcqe8xa1g2tljvcJeSqrqJVfYfvIH4lgKDC%2BUGdVd246bGzBH9Dyfxp3Z4vyUvNPBhY52XmOHYSgrrXEJPJpqeyGNbQv9KzDT%2F%2B%2BYBjqyAoA4JTLavy77oKQTVf6xhW4%2FMD371XZxf2cb18lNOSst3us3iC4OcBl2zDfjAbveu6uEnVLwjKa8kbHKfMBFtLI0hG9FrhzJE9Gu7eT1qm45VlzDpYLkSksYl%2FtaoGQ12ROlkUoy9XhKNXyfWow81tvrhq8RyXjqGWYr9cdNGi9AOLN01St%2BduNt71y8Yq%2BrFtJC0Hbbfl%2FCnHM7kPNssn%2BjvDigc5AKKbdnfuL9e6jg0A1xBhYKGCFBrwB9eAxIZwtlVQqIp%2B3%2FO5f72qdJZxzP%2BwpuV%2FGHXCm%2BDaQ1TnXIMIE9RjbpqpxoZmGkWAS3ZJlx9WvEBArOWFNN21q8O1qDRhbeSFztSRu2pva17IzwvKSoikSf1yDVZGtAw8WdLu6Z2muE3Cm6Ycn6bsaLmliQKg%3D%3D&X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Date=20220910T135311Z&X-Amz-SignedHeaders=host&X-Amz-Expires=43200&X-Amz-Credential=ASIAXKXYK7UONDOGS7GS%2F20220910%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Signature=2e529aec5c464b6314780daf07f0168f0e797880ccd93a20b3f8c76dd582ee3a'

def transform():
    """Perform basic aggregation of data and export data summary to csv"""
    pass
    # ---------- ADD CODE FOR TRANSFORM() HERE ---------- #
    

def load():
    """Load data to parquet file"""
    pass
    # ---------- ADD CODE FOR LOAD() HERE ---------- #
