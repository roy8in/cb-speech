from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page()
    page.goto('https://www.ecb.europa.eu/press/key/html/index.en.html', wait_until='networkidle')
    try:
        # wait for some content inside foedb-plugin
        page.wait_for_selector('.foedb-plugin div[class*="item"]', timeout=10000)
    except Exception as e:
        print("Timeout waiting for items", e)
    with open('ecb_loaded.html', 'w') as f:
        f.write(page.content())
    browser.close()
