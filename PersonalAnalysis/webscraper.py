from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By  # Import By
import time
import pandas as pd

def getRatings(card):
    rating = card.find_element(By.CLASS_NAME, "search_ratings").text
    return rating

def getRank(card):
    ranking = card.find_element(By.CLASS_NAME, "genre_rank").text
    return ranking

def getTitle(card):
    title = card.find_element(By.CLASS_NAME, "search_title").text
    return title

def getStats(card):
    stats = card.find_element(By.CLASS_NAME, "search_stats")
    return stats.text

def scanPage(pageNum, df):
    # Set up Chrome options
    options = Options()
    # options.add_argument("--headless")  # Run without GUI (optional)
    options.add_argument("--disable-blink-features=AutomationControlled")  # Helps bypass bot detection

    # Specify the correct path to ChromeDriver
    service = Service("/opt/homebrew/bin/chromedriver")  # Path from Homebrew

    # Initialize the WebDriver correctly
    driver = webdriver.Chrome(service=service, options=options)

    # Test if Chrome opens
    driver.get("https://www.novelupdates.com/series-ranking/?rank=popular&pg=" + str(pageNum))
    time.sleep(1)
    cards = driver.find_elements(By.CLASS_NAME, "search_main_box_nu")
    print(len(cards))

    for card in cards:
        ratings = getRatings(card)
        splitRatings = ratings.split(" ")
        country = splitRatings[0]
        stars = splitRatings[1].strip('()')
        rank = getRank(card)
        title = getTitle(card).split(rank)[1]
        stats = getStats(card)
        chapters = stats.split(" Every ")[0]
        stats = stats.split(" Every ")[1]
        release = stats.split(" Day(s) ")[0]
        stats = stats.split(" Day(s) ")[1]
        readerCount = stats.split(" Readers ")[0]
        stats = stats.split(" Readers ")[1]
        reviewCount = stats.split(" Reviews ")[0]
        lastPublished = stats.split(" Reviews ")[1]
        df.loc[rank] = [rank, title, country, stars, chapters, release, readerCount, reviewCount, lastPublished]

    driver.quit()

COLUMNS = ["Rank", "Title", "Country of Origin", "Star Rating", "Number of Chapters", "Release Frequency (Days)", "Number of Readers", "Number of Reviews", "Last Chapter Date of Publication"]
df = pd.DataFrame(columns=COLUMNS)

#1155 pages total
for i in range(1,1155):
    scanPage(i, df)

# scanPage(1, df)

df.to_csv("webnovel_2025_analysis.csv", header=True, index=False)