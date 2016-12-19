import os
from PyPDF2 import PdfFileReader, PdfFileMerger
import PyPDF2

files_dir = "C:\\Users\\User\\Desktop\\New folder"

pdf_in = open((os.path.join(files_dir, "20161121172718847.pdf")), 'rb')
pdf_reader = PyPDF2.PdfFileReader(pdf_in)
pdf_writer = PyPDF2.PdfFileWriter()
 
for pagenum in range(pdf_reader.numPages):
    print(pagenum)
    page = pdf_reader.getPage(pagenum)
    page.rotateClockwise(270)
    pdf_writer.addPage(page)

pdf_out = open('rotated.pdf', 'wb')
pdf_writer.write(pdf_out)
pdf_out.close()
pdf_in.close()

pdf_files = [f for f in os.listdir(files_dir) if f.endswith("pdf")]
merger = PdfFileMerger(strict=False)

for filename in pdf_files:
    if filename != 'receipts':
        print(filename)
        merger.append(PdfFileReader(os.path.join(files_dir, filename), "rb"))

merger.write(os.path.join(files_dir, "merged_full1.pdf"))
