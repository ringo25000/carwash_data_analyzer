from playwright.sync_api import sync_playwright

LOGIN_URL = "https://www.mycryptopay.com/login/index.php"

def main():
    with sync_playwright() as p:
        # Visible browser so you can log in
        browser = p.chromium.launch(headless=False)
        context = browser.new_context()
        page = context.new_page()

        # Go to login page
        page.goto(LOGIN_URL)

        print(">>> A Chromium window just opened.")
        print(">>> Log in as normal (username/password, any button captcha, etc.).")
        print(">>> Navigate to your Purchases page once you're logged in.")
        input("When you are fully logged in and on any logged-in page, press ENTER here...")

        # Save cookies/localStorage/etc. to a file
        context.storage_state(path="cryptopay_state.json")
        print("✅ Saved logged-in state to cryptopay_state.json")

        browser.close()

if __name__ == "__main__":
    main()
