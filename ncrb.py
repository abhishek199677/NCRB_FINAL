import scrapy
from scrapy import Request
from scrapy.crawler import CrawlerProcess
from pathlib import Path
import pandas as pd
from scrapy.shell import inspect_response

class NcrbSpider(scrapy.Spider):
    name = "ncrb"
    data = []

    def start_requests(self):
        base_url = 'https://ncrb.gov.in/accidental-deaths-suicides-in-india-table-content.html'
        yield Request(
            url=base_url,
            callback=self.parse_years,
        )

    def parse_years(self, response):
        # inspect_response(response, self)
        years = response.css('.js-selectYear option::text').extract()
        categories = response.css('.js-selectCategory option::text')[1:].extract()
        for year in years:
            for category in categories:
                url = f"{response.url}?year={year}&category={category}"
                yield Request(url=url, callback=self.parse_category, meta={'year': year, 'category': category})

    def parse_category(self, response):
        category = response.meta['category']
        pdf_links = response.css('a[href$=".pdf"]::attr(href)').extract()
        pdf_titles = response.css('a[href$=".pdf"]::text').extract()
        pdf_titles_cleaned = [title.split('_', 1)[-1].strip() for title in pdf_titles]  # Extract only the title after the first underscore
        print("PDF Titles:", pdf_titles_cleaned)
        pdf_data = [{'Title': title, 'Link': link} for title, link in zip(pdf_titles_cleaned, pdf_links)]
        self.data.append({
            'Category': category,
            'PDFs': pdf_data
        })

        for title, pdf_link in zip(pdf_titles_cleaned, pdf_links):
            yield Request(url=pdf_link, callback=self.save_pdf, meta={'category': category, 'title': title})

        next_page_url = response.css('.js-selectYear option[selected="selected"] + .js-selectCategory option[selected="selected"] + .chosen-container + .chosen-container + .chosen-drop a::attr(href)').extract_first()
        if next_page_url:
            yield Request(url=next_page_url, callback=self.parse_category, meta={'category': category})

    def save_pdf(self, response):
        category = response.meta['category']
        title = response.meta['title']
        folder_path = "/Users/apple/Desktop/NCRB_SCRAPING"
        Path(folder_path).mkdir(parents=True, exist_ok=True)
        clean_title = ''.join(c if c.isalnum() or c in ['-', '_', ' '] else '_' for c in title)
        file_path = f"{folder_path}/{clean_title}.pdf"  # Removed category and year from file name
        with open(file_path, 'wb') as file:
            file.write(response.body)
        self.log(f'Saved file {clean_title}.pdf in {folder_path}')

    def closed(self, reason):
        df = pd.DataFrame(self.data)
        self.save_csv(df, 'ncrb_data')

    # def save_csv(self, df, title):
    #     folder_path = "/Users/apple/Desktop/FACTLY"
    #     Path(folder_path).mkdir(parents=True, exist_ok=True)
    #     # file_path = f"{folder_path}/{title}.csv"
        # df.to_csv(file_path, index=False)


def main():
    process = CrawlerProcess()
    process.crawl(NcrbSpider)
    process.start()


if __name__ == '__main__':
    main()