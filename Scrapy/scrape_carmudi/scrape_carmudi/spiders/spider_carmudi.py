import scrapy


class SpiderCarmudiSpider(scrapy.Spider):
    name = 'spider_carmudi'
    allowed_domains = ['www.carmudi.co.id']

    origin_url = 'https://www.carmudi.co.id/mobil-bekas-dijual/indonesia'
    start_urls = [origin_url]

    # parse method is the main driver of a spider
    def parse(self, response):

        urls = response.css('h2 > a[class="ellipsize  js-ellipsize-text"]::attr("href")').getall()
        for url in urls:
            yield scrapy.Request(url=url, callback=self.parse_details)

        next_page = response.css('li.next > a::attr("href")').extract_first()
        if next_page:
            next_page = response.urljoin(next_page)
            yield scrapy.Request(url=next_page, callback=self.parse)

    def parse_details(self, response):

        # Getting the brand, model and type
        brand = response.css('span[itemprop="name"]::text').getall()[2]
        model = response.css('span[itemprop="name"]::text').getall()[3]
        type = response.css('span[itemprop="name"]::text').getall()[4] 

        # Getting additional information
        year = response.css('span[class="u-text-bold  u-block"]::text').getall()[1]
        km = response.css('span[class="u-text-bold  u-block"]::text').getall()[2]
        color = response.css('span[class="u-text-bold  u-block"]::text').getall()[3]

        # Getting the specification
        field = response.css('div[class="o-grid  o-grid--lg  u-margin-ends-lg"]')

        specs = {}

        for i in field:
            title = i.css('h3::text').extract_first()
            details = i.css('span::text').getall()
            spec = {}
            for n, i in enumerate(details):
                if n % 2 == 0:
                    head = i
                else:
                    body = i
                    spec[head] = body
            specs[title] = spec

        # getting the price
        price_cash =  response.css('div[class="listing__price  u-text-4  u-text-bold"]::text').get()[3:]


        yield {
            'brand': brand,
            'model': model,
            'type': type,
            'year': year,
            'km': km,
            'color': color, 
            'specs': specs,
            'price_cash': price_cash,
        }