from playwright.sync_api import sync_playwright
import time

urls = []
def handle_response(response):
    if any(x in response.url for x in [".json", "api", "foedb"]):
        urls.append(response.url)

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.on("response", handle_response)
    page.goto('https://www.ecb.europa.eu/press/key/html/index.en.html', wait_until='domcontentloaded')
    time.sleep(5)
    browser.close()

print("Interesting URLs:")
for u in urls: print(u)
