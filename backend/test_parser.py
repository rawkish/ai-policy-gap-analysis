import logging
from services.pdf_parser import *

print("Starting parse_pdf...")
chunks = parse_pdf('assets/security-control-policy.pdf', 'test.pdf')
print("Chunks:", len(chunks))
