# import pdfrw
import PyPDF2
import os
import re
import datetime


class Globals:
    def __init__(self):
        # always 202
        self.appid = '202'
        self.corr_year = '2018'
        # mailing date
        self.scan_date = {"1": "11/12/18", "2A": "11/12/18", "3A": "11/14/18",
                          "3B": "11/12/18", "3C": "11/12/18", "3D": "11/12/18",
                          "4A": "11/12/18", "4B": "11/12/18", "4C": "11/12/18",
                          "4D": "11/12/18", "5A": "11/13/18", "5B": "11/12/18",
                          "5C": "11/12/18", "5D": "11/12/18", "6A": "11/12/18",
                          "6B": "11/12/18", "6C": "11/12/18", "6D": "11/12/18",
                          "8": "11/12/18", "9A": "11/12/18", "9B": "11/12/18",
                          "10A": "11/12/18", "11A": "11/12/18", "11B": "11/12/18",
                          "12A": "11/12/18", "12B": "11/12/18"}

        # bucket state
        self.state = {"1": "IA", "2A": "IA", "3A": "IA", "3B": "IA",
                      "3C": "IA", "3D": "IA", "4A": "IA", "4B": "IA",
                      "4C": "IA", "4D": "IA", "5A": "IA", "5B": "IA",
                      "5C": "IA", "5D": "IA", "6A": "IA", "6B": "IA",
                      "6C": "IA", "6D": "IA", "8": "SD", "9A": "SD",
                      "9B": "SD", "10A": "SD", "11A": "SD", "11B": "SD",
                      "12A": "SD", "12B": "SD"}


def get_wellmark_id(pdf_file_path):

    pdf_file_obj = open(pdf_file_path, 'rb')
    pdf_reader = PyPDF2.PdfFileReader(pdf_file_obj)

    all_pages = [i for i in range(0, pdf_reader.numPages)]
    name_pages = ([i for i in range(0, pdf_reader.numPages) if ((i + 1) % 4) == 0])

    batch = PyPDF2.PdfFileWriter()
    wid_re = re.compile("\d{3}(AD)\d{4}")
    # seq = 1
    for n, i in enumerate(all_pages, 1):
        page_obj = pdf_reader.getPage(i)
        batch.addPage(pdf_reader.getPage(i))
        if i in name_pages:
            text = page_obj.extractText()
            srch = wid_re.search(text)
            srch_cnt = wid_re.findall(text)

            if srch is None:
                print("Skipping: {0} Record: {1}\n{2}\n\n".format(os.path.basename(pdf_file_path), i, text))
            if len(srch_cnt) > 1:
                print(("WARNING!!! Mulitple Matches!!!: "
                       "{0} Record: {1}\n{2}\n\n".format(os.path.basename(pdf_file_path), i, text)))

        # if seq >= 5: break

    pdf_file_obj.close()


def process_pdf(pdf_file_path, bucket, show_page_lists=False):
    print("Processing: {0}".format(os.path.basename(pdf_file_path)))

    # get global settings
    g = Globals()

    # compile regular expressions for searches
    # state_re = re.compile("[a-z, A-Z][a-z, A-Z](?=_Bucket)")
    # bucket_re = re.compile("(?<=Bucket[\s])[\S\s]|[A-Z](?=_)")
    # wid_re = re.compile("\d{3}(AD)\d{4}|(W)\d{8}")
    wid_re = re.compile("(?!111AD1111)(\d{3}(AD)\d{4}|(W)\d{8})")
    date_string = datetime.datetime.strftime(datetime.datetime.today(), "%m%d%Y%H%M%S")
    # 
    save_dir_name = ('jttocust100001_{state}{bucket}_{timestamp}'.format(bucket=bucket,
                                                                         timestamp=date_string,
                                                                         state=Globals().state[bucket]))

    # Add primary folder
    save_dir_name = os.path.join(save_dir_name, '0')

    # make a new directory to save results in
    if not os.path.exists(save_dir_name) and not show_page_lists:
        os.makedirs(save_dir_name)

    # open the pdf
    pdf_file_obj = open(pdf_file_path, 'rb')
    pdf_reader = PyPDF2.PdfFileReader(pdf_file_obj)

    # make lists of all pages, pages to search WID on, last page of each record
    all_pages = set(i for i in range(0, pdf_reader.numPages))
    wid_search_pages = set(i for i in range(0, pdf_reader.numPages) if (i % 2) == 0)
    doc_last_pages = set(i for i in range(0, pdf_reader.numPages) if (i % 2) == 1)

    # Yes, above could be done more efficiently (because each record is 2 pages), 
    #   but I decided to go with an explicit list as a framework for projects
    #   with more than two pages per record.

    # a little condition for debugging
    if show_page_lists:
        print("** Debug page lists, full processing not done **")
        print("all pages: ", all_pages)
        print("name pages: ", wid_search_pages)
        print("last pages: ", doc_last_pages)
        pdf_file_obj.close()
        exit()

    # initialize a couple of variables
    batch = PyPDF2.PdfFileWriter()
    extracted_wid = None
    seq = 0
    for n, i in enumerate(all_pages, 1):
        # where n is the iteratator count, i is the source pdf page number
        page_obj = pdf_reader.getPage(i)
        batch.addPage(pdf_reader.getPage(i))

        # Create secondary folder
        secondary_dir = int(seq / 100000)
        secondary_dir = os.path.join(save_dir_name, str.zfill(str(secondary_dir), 2))
        if not os.path.exists(secondary_dir):
            os.mkdir(secondary_dir)
        #

        if i in wid_search_pages:
            # search for text, save to variable
            text = page_obj.extractText()
            srch = wid_re.search(text)
            srch_cnt = wid_re.findall(text)

            if len(srch_cnt) > 2:
                print(("WARNING!!! Too Many Matches!!!: "
                       "{0} Record: {1}\n{2}\n\n".format(os.path.basename(pdf_file_path), i, text)))

            if srch is not None:
                extracted_wid = srch[0]
            else:
                print("Skipping: {0} Record: {1}\n{2}\n\n".format(os.path.basename(pdf_file_path), i, text))

        if (i in doc_last_pages) and (i != pdf_reader.numPages):
            # write dat file, write out to pdf
            with open(os.path.join(secondary_dir, "{0:0>5}001.pdf".format(seq)), 'wb') as output:
                batch.write(output)
            with open(os.path.join(secondary_dir, "{0:0>5}IDX.dat".format(seq)), 'w') as datfile:
                datfile.write("{appid};1;;;;;;;;;;;{wid};0001;N;{year};{scan}\n".format(wid=extracted_wid,
                                                                                        appid=g.appid,
                                                                                        scan=g.scan_date[bucket],
                                                                                        year=g.corr_year))
            seq += 1
            batch = PyPDF2.PdfFileWriter()

        if seq >= 10: break

    pdf_file_obj.close()


if __name__ == '__main__':

    process_folders = [d for d in os.listdir(os.chdir("..")) if os.path.isdir(d) and d != 'kovis']
    for n, folder in enumerate(process_folders, 1):
        # where n is the iteration count
        process_path = os.path.join(os.path.abspath("."), folder)

        print_re = re.compile("(Bucket)")

        bucket = os.path.basename(process_path)[7:]
        pdf_print_files = [f for f in os.listdir(process_path) if print_re.search(f) and f[-3:].upper() == 'PDF']

        pdf_print_files = map(lambda f: os.path.abspath(os.path.join(process_path, f)), pdf_print_files)
        #
        for pdf in pdf_print_files:
            process_pdf(pdf, bucket)

        if n >= 1: break
