from playwright.sync_api import sync_playwright
import json
import os

PURCHASES_URL = "https://www.mycryptopay.com/login/index.php?page=purchases"
STATE_FILE = "cryptopay_state.json"
OUTPUT_FILE = "cryptopay_allData.json"


def get_max_page(page) -> int:
    # """
    # Look at the 'Page: 1, 2, 3 ... N' area and return the largest page number.
    # """
    # max_page = page.evaluate(
    #     """
    #     () => {
    #       const ps = Array.from(document.querySelectorAll("p"))
    #         .filter(p => /Page:/i.test(p.textContent));
    #       if (!ps.length) return 1;
    #       const pager = ps[ps.length - 1];
    #       const spans = Array.from(pager.querySelectorAll("span"));
    #       let maxNum = 1;
    #       for (const s of spans) {
    #         const t = s.textContent.trim();
    #         const n = parseInt(t, 10);
    #         if (!Number.isNaN(n) && n > maxNum) maxNum = n;
    #       }
    #       return maxNum;
    #     }
    #     """
    # )
    return int(1) #int(max_page or 1)
    # For testing only, you can temporarily do:
    # return 2


def scrape_page(page, page_num: int):
    """
    Use the site's own JS functions to switch to page `page_num`,
    then scrape that page's purchases and return a list of dicts.
    For each entry, we keep checking until a non-empty transaction_id is found
    (or we hit a max number of attempts).
    """
    return page.evaluate(
        """
    async (pageNum) => {
      const sleep = (ms) => new Promise(res => setTimeout(res, ms));

      // Switch the internal 'pagenum' and reload purchases_inner, just like the span onclick
      if (typeof setAdditionalVar === "function" && typeof selectTab === "function") {
        setAdditionalVar("pagenum", String(pageNum));
        selectTab("purchases_inner");
        await sleep(2000);  // give the table time to refresh
      }

      // There may be multiple purchases tables if the site appends instead of replacing.
      // Always use the LAST one (the most recent page).
      const tables = document.querySelectorAll("table.purchases-table");
      if (!tables.length) return [];
      const purchasesTable = tables[tables.length - 1];

      const cells = Array.from(
        purchasesTable.querySelectorAll("td.purchase-transaction")
      );

      const allData = [];

      for (const cell of cells) {
        const innerTable = cell.querySelector("table");
        if (!innerTable) continue;

        const row = innerTable.querySelector("tr");
        if (!row) continue;

        const tds = row.querySelectorAll("td");
        if (tds.length < 4) continue;

        const dateTime   = tds[0].innerText.trim();
        const cardholder = tds[1].innerText.trim();
        const type       = tds[2].innerText.trim();
        const totalStr   = tds[3].innerText.trim();

        // CLICK row to load dropdown
        innerTable.scrollIntoView({ block: "center" });
        innerTable.click();

        let detailsText = "";
        let detailRows = [];
        let transactionId = "";

        // Keep polling until we see a non-empty Transaction ID or hit max attempts
        for (let attempt = 0; attempt < 30 && !transactionId; attempt++) {
          await sleep(200);  // 30 * 200ms = 6000ms max

          // 1) Try inside same cell
          let detailsDiv = cell.querySelector("div[id^='transaction_pos_']");

          // 2) If not found, try next <tr> sibling (very common expandable-row pattern)
          if (!detailsDiv) {
            const tr = cell.closest("tr");
            if (tr && tr.nextElementSibling) {
              const nextTr = tr.nextElementSibling;
              const candidate = nextTr.querySelector("div[id^='transaction_pos_']");
              if (candidate) {
                detailsDiv = candidate;
              }
            }
          }

          if (!detailsDiv) {
            continue;  // nothing visible yet, keep waiting
          }

          const detailsTable = detailsDiv.querySelector("table");
          const text = (detailsTable ? detailsTable.innerText : detailsDiv.innerText || "").trim();
          if (!text) {
            continue;  // still loading / empty
          }

          detailsText = text;

          if (detailsTable) {
            detailRows = Array.from(detailsTable.rows).map(r =>
              Array.from(r.cells).map(td => td.innerText.trim())
            );
          }

          const m = text.match(/Transaction ID[:\\s]+(\\d+)/i);
          if (m) {
            transactionId = m[1];
            break;
          }
        }

        // Optional debug if we *never* saw a transactionId (ideally shouldn't happen)
        if (!transactionId) {
          console.log("WARN: No transaction ID found for row with datetime:", dateTime, "cardholder:", cardholder);
        }

        allData.push({
          "datetime": dateTime,
          "cardholder": cardholder,
          "type": type,
          "total": totalStr,
          "transaction_id": transactionId,
          "details_text": detailsText,
        });
      }

      return allData;
    }
    """,
        page_num,
    )


def load_existing_entries():
    if not os.path.exists(OUTPUT_FILE):
        return []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def make_key(entry):
    """
    Unique-ish key for an entry (for incremental dedupe).
    """
    txid = (entry.get("transaction_id") or "").strip()
    dt = (entry.get("datetime") or "").strip()
    cardholder = (entry.get("cardholder") or "").strip()
    total = (entry.get("total") or "").strip()
    return (txid, dt, cardholder, total)


def scrape_all_data():
    """
    Full scrape: go through every page and return all entries.
    Use this on first run when no JSON exists yet.
    """
    with sync_playwright() as p:
        # For debugging, you can do headless=False, slow_mo=200
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=STATE_FILE)
        page = context.new_page()

        page.goto(PURCHASES_URL, wait_until="networkidle")

        max_page = get_max_page(page)
        print(f"Detected {max_page} pages of purchases")

        all_data = []

        for page_num in range(1, max_page + 1):
            print(f"Scraping page {page_num}/{max_page} ...")
            page_data = scrape_page(page, page_num)
            print(f"  -> {len(page_data)} purchases on page {page_num}")
            all_data.extend(page_data)

        browser.close()
        return all_data


def incremental_update():
    """
    Incremental scrape:
    - Load existing JSON.
    - Get the latest known transaction_id (from entries[0]).
    - Scrape from page 1 onward, collecting new entries.
    - As soon as we hit a row whose transaction_id matches the latest known one,
      we stop (everything after is older).
    - Use known_keys as a safety net against accidental duplicates.
    """
    existing_entries = load_existing_entries()
    print(f"Loaded {len(existing_entries)} existing entries.")

    latest_txid = None
    if existing_entries:
        latest_txid = (existing_entries[0].get("transaction_id") or "").strip() or None
    print(f"Latest known transaction_id: {latest_txid!r}")

    known_keys = {make_key(e) for e in existing_entries}
    new_entries = []
    hit_boundary = False

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(storage_state=STATE_FILE)
        page = context.new_page()

        page.goto(PURCHASES_URL, wait_until="networkidle")

        max_page = get_max_page(page)
        print(f"Detected {max_page} pages of purchases")

        for page_num in range(1, max_page + 1):
            if hit_boundary:
                break

            print(f"Scraping page {page_num}/{max_page} ...")
            page_data = scrape_page(page, page_num)
            print(f"  -> {len(page_data)} purchases on page {page_num}")

            for entry in page_data:
                txid = (entry.get("transaction_id") or "").strip()

                # If we’ve reached the last known transaction, stop.
                if latest_txid and txid == latest_txid:
                    print("Hit latest known transaction_id – stopping incremental scrape.")
                    hit_boundary = True
                    break

                key = make_key(entry)
                if key in known_keys:
                    continue  # already have this one (safety net)

                known_keys.add(key)
                new_entries.append(entry)

        browser.close()

    if new_entries:
        print(f"Found {len(new_entries)} new entries.")
        # Newest first: freshly scraped (new → old) + existing (already new → old)
        all_entries = new_entries + existing_entries
    else:
        print("No new entries found.")
        all_entries = existing_entries

    return all_entries



if __name__ == "__main__":
    # If we've never scraped before, do a full scrape.
    # Otherwise, only scrape new stuff and merge.
    if not os.path.exists(OUTPUT_FILE):
        print("No existing data file found – doing full initial scrape...")
        data = scrape_all_data()
    else:
        print("Existing data file found – doing incremental update...")
        data = incremental_update()

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)

    print(f"Saved {len(data)} purchases to {OUTPUT_FILE}")
