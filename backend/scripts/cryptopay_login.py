from pathlib import Path
from playwright.sync_api import sync_playwright

LOGIN_URL = "https://www.mycryptopay.com/login/index.php"

# Resolve: backend/scripts → parent is backend → /data
DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR.mkdir(exist_ok=True)  # make sure it exists

STATE_PATH = DATA_DIR / "cryptopay_state.json"

def main():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        page.goto(LOGIN_URL)

        print(">>> A Chromium window just opened.")
        print(">>> Log in as normal (username/password, any button captcha, etc.).")
        print(">>> Navigate to your Purchases page once you're logged in.")
        input("When you are fully logged in and on any logged-in page, press ENTER here...")

        # Save cookies/localStorage/etc. to backend/data/cryptopay_state.json
        context.storage_state(path=str(STATE_PATH))
        print(f"✅ Saved logged-in state to {STATE_PATH}")

        browser.close()

if __name__ == "__main__":
    main()
