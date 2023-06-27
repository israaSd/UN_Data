from Class import HDXScraper
import sys 

# Create and configure logger

                    
# logger = logging.getLogger()

if __name__ == "__main__":
    scraper = HDXScraper()
    scraper.get_hdx_datasets()
    scraper.clear_duplicates()
    scraper.multi_download(scraper.datasets)
    