import re
import aiohttp
import asyncio
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import time
import json
from urllib.robotparser import RobotFileParser

class ProductCrawler:
    def __init__(self, base_url, user_agent='*'):
        self.base_url = base_url
        self.visited_urls = set()
        self.product_urls = set()
        self.queue = [base_url]
        self.user_agent = user_agent
        self.robot_parser = RobotFileParser()
        
        # Common product URL patterns (add more as needed)
        self.product_patterns = [
            re.compile(r'/product/'),
            re.compile(r'/products/'),
            re.compile(r'/p/'),
            re.compile(r'/prod/'),
            re.compile(r'/item/'),
            re.compile(r'\?product_id='),
            re.compile(r'/dp/'),
            re.compile(r'/products/[a-z0-9-]+', re.IGNORECASE),  # New pattern for URLs like /products/positive-affirmations-pencils-pack-of-10
            # re.compile(r'/[A-Z0-9]{10}', re.IGNORECASE)  # Common Amazon-style ASIN pattern
        ]

    def is_allowed(self, url):
        """Check robots.txt for crawling permissions"""
        parsed_url = urlparse(url)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        self.robot_parser.set_url(robots_url)
        try:
            self.robot_parser.read()
            return self.robot_parser.can_fetch(self.user_agent, url)
        except Exception as e:
            print(f"Error checking robots.txt: {e}")
            return False
        

    def get_crawl_delay(self):
        """Get crawl delay from robots.txt"""
        if self.robot_parser.crawl_delay(self.user_agent) is not None:
            return self.robot_parser.crawl_delay(self.user_agent)
        else:
            return 1

    def is_product_url_method1(self, url, soup):
        """Check if URL matches any product pattern"""

        start_time = time.time()

        # ---- URL pattern check ----
        url_score = 0
        for pattern in self.product_patterns:
            if pattern.search(url):
                url_score = 0.3
                break

        
        # ---- HTML content check ----
        content_score=0

        # Check for Add to Cart button
        if soup.find(string=re.compile(r'add to cart', re.IGNORECASE)):
            content_score += 0.3

        # Check for price elements
        if soup.find(class_=re.compile(r'price', re.IGNORECASE)):
            content_score += 0.3

        #Check for product description keywords
        if re.search(r'product description|details|features|specifications', soup.text, re.IGNORECASE):
            content_score += 0.3

        # ---- Schema.org check ----
        if soup.find('script', type='application/ld+json'):
            try:
                structured_data = json.loads(soup.find('script', type='application/ld+json').string)
                if structured_data.get('@type') == 'Product':
                    print("Structured data found")
                    content_score += 0.8
            except (json.JSONDecodeError, AttributeError):
                pass

        
        # print(f"URL score: {url_score}, Content score: {content_score}")
        # ---- Final decision ----
        total_score = url_score + content_score

        end_time = time.time()
        print(f"URL: {url}, Score: {total_score}, Time: {end_time - start_time:.2f} seconds")

        if total_score >= 0.7:
            return True
        else:
            return False
        

    def is_product_url_method2(self,url):
        """Check if URL matches any product pattern"""

        # define based url and product url mapping
        # Mapping of base URLs to their respective product URL regex patterns
        BASE_URL_TO_PRODUCT_PATTERN = {
            "pyarababy.com": re.compile(r'/products/[a-z0-9-]+', re.IGNORECASE),  # Example: products/baybee-cradle-for-baby
            "myntra.com": re.compile(r'/[^/]+/[^/]+/[^/]+/\d+/buy', re.IGNORECASE),       # Example: oundation/orgatre/orgatre-mood-bliss-foundation-for-full-coverage-oil-control-matte---30-ml--shade--138/32280543/buy
            "firstcry.com": re.compile(r'/[^/]+/[^/]+/\d+/product-detail'),    # Example: momisy/momisy-regular-length-socks-pack-of-5-checks-flower-red-and-yellow/13196960/product-detail
            "amazon.in": re.compile(r"/dp/[A-Z0-9]{10}"),  # Example: /dp/B08XYZ1234
            "bewakoof.com": re.compile(r'/p/[a-zA-Z0-9-]+(?:-for-)?[a-zA-Z0-9-]+', re.IGNORECASE),  # Example: /p/itachi-blood-premium-glass-cover-for-apple-iphone-15-plus
            "thesouledstore.com": re.compile(r'/product/[a-zA-Z0-9-]+(?:\?[a-zA-Z0-9=&]+)?', re.IGNORECASE),
        }

        # Parse the URL to extract the base URL (netloc)
        parsed_url = urlparse(url)
        base_url = parsed_url.netloc  # e.g., "example1.com"


        # Remove "www." if present
        if base_url.startswith("www."):
            base_url = base_url[4:]


        # Get the product URL regex pattern for the base URL
        product_pattern = BASE_URL_TO_PRODUCT_PATTERN.get(base_url)

        if product_pattern is None:
            # No pattern defined for this base URL
            return False
        


        # Check if the URL path matches the product URL pattern
        return bool(product_pattern.search(parsed_url.path))



    async def parse_link_check_product_url(self, url, link, soup):
        full_url = urljoin(url, link['href'])
        parsed_url = urlparse(full_url)

        # Clean URL parameters and fragments
        clean_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"

        if clean_url not in self.visited_urls:
            return {
                "is_product_url": self.is_product_url_method2(clean_url),
                "link": clean_url
            }
        else:
            return None

    async def get_links(self, session, url):
        """Extract all links from a page"""
        try:
            headers = {'User-Agent': 'Mozilla/5.0 (compatible; ProductCrawler/1.0)'}
            async with session.get(url, headers=headers, timeout=10) as response:
                response.raise_for_status()
                html_content = await response.text()
            
            soup = BeautifulSoup(html_content, 'html.parser')
            links = []

            start_time = time.time()
        

            links = await asyncio.gather(*[self.parse_link_check_product_url(url, link, soup) for link in soup.find_all('a', href=True)])
                    
            end_time = time.time()
            print(f"Found {len(links)} links in {end_time - start_time:.2f} seconds")

            return links
        except Exception as e:
            print(f"Error fetching {url}: {e}")
            return []

    async def crawl(self, max_pages=50):
        """Main crawling function"""
        start_time = time.time()  # Start timing
        async with aiohttp.ClientSession() as session:
            while self.queue and len(self.visited_urls) < max_pages:
                current_url = self.queue.pop(0)
                
                if current_url in self.visited_urls:
                    continue
                    
                if not self.is_allowed(current_url):
                    print(f"Skipping {current_url} due to robots.txt restrictions")
                    continue
                    
                print(f"Crawling: {current_url}")
                
                self.visited_urls.add(current_url)
                
                print("Going to fetch links")   
                # Get and process new links
                new_links = await self.get_links(session, current_url)
                for link_detail in new_links:
                    if link_detail is None:
                        continue
                    if link_detail["link"] not in self.visited_urls and link_detail["link"] not in self.queue:
                        self.queue.append(link_detail["link"])

                    if link_detail["is_product_url"]:
                        self.product_urls.add(link_detail["link"])

                # Respect crawl delay
                crawl_delay = self.get_crawl_delay()
                print(f"Waiting {crawl_delay} seconds before next request")
                await asyncio.sleep(crawl_delay)  # add delay
                
        end_time = time.time()  # End timing
        elapsed_time = end_time - start_time
        print(f"Crawling completed in {elapsed_time:.2f} seconds")
        
        return list(self.product_urls)

async def main():
    domain_list = [ "https://www.bewakoof.com/", "https://www.pyarababy.com/",  "https://www.firstcry.com/" ]

    max_pages = 100

    results = []
    # Run parallel crawlers for each domain
    for domain in domain_list:
        crawler = ProductCrawler(base_url=domain)
        results.append(crawler.crawl(max_pages=max_pages))

    product_urls = await asyncio.gather(*results)
    # write all the product urls to a file inside output folder in json format along with domain name
    all_urls = {}
    for domain, urls in zip(domain_list, product_urls):
        all_urls[domain] = urls
        print(f"Found {len(urls)} product URLs for {domain}")
    
    with open("output/product_urls.json", 'w') as f:
        json.dump(all_urls, f, indent=4)


if __name__ == "__main__":
    asyncio.run(main())
    
