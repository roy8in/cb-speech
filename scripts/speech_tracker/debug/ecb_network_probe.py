from playwright.sync_api import sync_playwright
import json

urls = []


def handle_response(response):
    if "json" in response.url or "foedb" in response.url or "api" in response.url or "search" in response.url:
        urls.append(response.url)


with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.on("response", handle_response)
    page.goto('https://www.ecb.europa.eu/press/key/html/index.en.html', wait_until='networkidle')
    browser.close()

print("Interesting URLs:")
for u in urls:
    print(u)
