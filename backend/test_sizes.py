import pdfplumber
from services.pdf_parser import _extract_page_elements

with pdfplumber.open('assets/security-control-policy.pdf') as pdf:
    for page in pdf.pages:
        elements, _ = _extract_page_elements(page)
        for e in elements:
            if e['type'] == 'line':
                print(f"{e['size']:.2f} : {e['text']}")
