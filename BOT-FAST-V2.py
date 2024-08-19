import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from bs4 import BeautifulSoup
from webdriver_manager.chrome import ChromeDriverManager
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_data(url):
    chrome_options = Options()
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(10)
    
    try:
        driver.get(url)
    except TimeoutException:
        driver.execute_script("window.stop();")
        driver.execute_script("document.dispatchEvent(new KeyboardEvent('keydown', {'key': 'Escape'}));")

    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_all_elements_located((By.CLASS_NAME, 'card'))
        )
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        cards = soup.find_all('div', class_='card')
        
        results = []
        for card in cards:
            try:
                if card.get('id') and card.get('data-link'):
                    span = card.find('span', class_='larger')
                    name = span.text.strip()
                    phone_number = None
                    phone_element = card.select_one('div[class="card-block"] > strong > a')
                    if phone_element:
                        phone_number = phone_element.text.strip()
                    results.append({'Name': name, 'Number': phone_number})
                    
            except AttributeError:
                continue
        
    except TimeoutException:
        print(f"Timeout occurred while processing URL: {url}")
        results = None
    
    finally:
        driver.quit()
    
    return results

def process_urls(urls):
    output_data = []
    for url in urls:
        print(f"Processing URL: {url}")
        results = extract_data(url)
        
        if results:
            for result in results:
                output_data.append({'URL': url, 'Name': result['Name'], 'Number': result['Number']})
        else:
            output_data.append({'URL': url, 'Name': 'N/A', 'Number': 'N/A'})
            
        output_df = pd.DataFrame(output_data)
        file_exists = os.path.isfile('output_results.csv')
        mode = 'a' if file_exists else 'w'  
        output_df.to_csv('output_results.csv', index=False, mode=mode, header=not file_exists)
        print("Extraction and processing completed. Results saved to output_results.csv.")
    return output_data

def main():
    csv_file = 'people.csv'
    df = pd.read_csv(csv_file)
    num_threads = 5
    urls = df['URL'].tolist()
    chunk_size = len(urls) // num_threads
    url_chunks = [urls[i:i + chunk_size] for i in range(0, len(urls), chunk_size)]
    all_results = []
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        future_to_chunk = {executor.submit(process_urls, chunk): chunk for chunk in url_chunks}
        
        for future in as_completed(future_to_chunk):
            result = future.result()
            all_results.extend(result)

    # Save results to CSV
    # output_df = pd.DataFrame(all_results)
    # output_df.to_csv('output_results_combined.csv', index=False)
    # print("All URLs processed. Results saved to output_results_combined.csv.")

if __name__ == '__main__':
    main()
